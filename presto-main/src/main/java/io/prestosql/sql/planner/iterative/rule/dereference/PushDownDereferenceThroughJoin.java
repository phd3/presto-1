/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.planner.iterative.rule.dereference;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.ExpressionNodeInliner;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Transforms:
 * <pre>
 *  Project(a_x := a.msg.x)
 *    Join(a_y = b_y) => [a]
 *      Project(a_y := a.msg.y, a := a)
 *          Source(a)
 *      Project(b_y := b.msg.y)
 *          Source(b)
 *  </pre>
 * to:
 * <pre>
 *  Project(a_x := symbol)
 *    Join(a_y = b_y) => [symbol]
 *      Project(symbol := a.msg.x, a_y := a.msg.y, a := a)
 *        Source(a)
 *      Project(b_y := b.msg.y)
 *        Source(b)
 * </pre>
 *
 * Pushes down dereference projections through JoinNode. Excludes dereferences on symbols being used in join criteria to avoid
 * data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferenceThroughJoin
        implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughJoin(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(join().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);

        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        expressionsBuilder.addAll(projectNode.getAssignments().getExpressions());
        joinNode.getFilter().ifPresent(expressionsBuilder::add);

        Map<DereferenceExpression, Symbol> dereferenceProjections = validDereferences(expressionsBuilder.build(), context, typeAnalyzer, true);

        // Exclude criteria symbols
        ImmutableSet.Builder<Symbol> criteriaSymbolsBuilder = ImmutableSet.builder();
        joinNode.getCriteria().forEach(criteria -> {
            criteriaSymbolsBuilder.add(criteria.getLeft());
            criteriaSymbolsBuilder.add(criteria.getRight());
        });
        Set<Symbol> excludeSymbols = criteriaSymbolsBuilder.build();

        // Consider dereferences in projections and join filter for pushdown
        Map<DereferenceExpression, Symbol> pushdownDereferences = dereferenceProjections.entrySet().stream()
                .filter(entry -> !excludeSymbols.contains(getBase(entry.getKey())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        Assignments.Builder leftSideDereferences = Assignments.builder();
        Assignments.Builder rightSideDereferences = Assignments.builder();

        // Separate dereferences coming from left and right nodes
        HashBiMap.create(pushdownDereferences).inverse().entrySet().stream()
                .forEach(entry -> {
                    Symbol baseSymbol = getBase(entry.getValue());
                    if (joinNode.getLeft().getOutputSymbols().contains(baseSymbol)) {
                        leftSideDereferences.put(entry.getKey(), entry.getValue());
                    }
                    else if (joinNode.getRight().getOutputSymbols().contains(baseSymbol)) {
                        rightSideDereferences.put(entry.getKey(), entry.getValue());
                    }
                    else {
                        throw new IllegalArgumentException(format("Unexpected symbol %s in projectNode", baseSymbol));
                    }
                });

        Assignments pushdownDereferencesLeft = leftSideDereferences.build();
        Assignments pushdownDereferencesRight = rightSideDereferences.build();

        PlanNode leftNode = createProjectNodeIfRequired(joinNode.getLeft(), pushdownDereferencesLeft, context.getIdAllocator());
        PlanNode rightNode = createProjectNodeIfRequired(joinNode.getRight(), pushdownDereferencesRight, context.getIdAllocator());

        // Prepare new assignments for project node
        Assignments newAssignments = projectNode.getAssignments().rewrite(
                new ExpressionNodeInliner(
                        pushdownDereferences.entrySet().stream()
                                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()))));

        // Prepare new output symbols for join node
        List<Symbol> referredSymbolsInAssignments = newAssignments.getExpressions().stream()
                .flatMap(expression -> SymbolsExtractor.extractAll(expression).stream())
                .collect(toList());

        List<Symbol> newLeftOutputSymbols = ImmutableList.<Symbol>builder()
                .addAll(joinNode.getLeftOutputSymbols())
                // Exclude new output symbols only used in filter
                .addAll(pushdownDereferencesLeft.getOutputs().stream()
                        .filter(referredSymbolsInAssignments::contains)
                        .collect(toImmutableList()))
                .build();

        List<Symbol> newRightOutputSymbols = ImmutableList.<Symbol>builder()
                .addAll(joinNode.getRightOutputSymbols())
                // Exclude new output symbols only used in filter
                .addAll(pushdownDereferencesRight.getOutputs().stream()
                        .filter(referredSymbolsInAssignments::contains)
                        .collect(toImmutableList()))
                .build();

        JoinNode newJoinNode = new JoinNode(context.getIdAllocator().getNextId(),
                joinNode.getType(),
                leftNode,
                rightNode,
                joinNode.getCriteria(),
                newLeftOutputSymbols,
                newRightOutputSymbols,
                // Use newly created symbols in filter
                joinNode.getFilter().map(expression -> replaceExpression(
                        expression,
                        pushdownDereferences.entrySet().stream()
                                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference())))),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newJoinNode, newAssignments));
    }

    private static PlanNode createProjectNodeIfRequired(PlanNode planNode, Assignments dereferences, PlanNodeIdAllocator idAllocator)
    {
        if (dereferences.isEmpty()) {
            return planNode;
        }
        return new ProjectNode(
                idAllocator.getNextId(),
                planNode,
                Assignments.builder()
                        .putIdentities(planNode.getOutputSymbols())
                        .putAll(dereferences)
                        .build());
    }
}
