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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.ExpressionNodeInliner;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.window;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(msg1_x := msg1.x, msg2_x := msg2.x, msg3_x := msg3.x, msg4_x := msg4.x)
 *      Window(orderBy = [msg2], partitionBy = [msg3], min_msg4 := min(msg4))
 *          Source(msg1, msg2, msg3, msg4)
 *  </pre>
 * to:
 * <pre>
 *  Project(msg1_x := symbol, msg2_x := msg2.x, msg3_x := msg3.x, msg4_x := msg4.x)
 *      Window(orderBy = [msg2], partitionBy = [msg3], min_msg4 := min(msg4))
 *          Project(msg1 := msg1, symbol := msg1.x, msg2 := msg2, msg3 := msg3, msg4 := msg4)
 *              Source(msg1, msg2, msg3, msg4)
 * </pre>
 *
 * Pushes down dereference projections through Window. Excludes dereferences on symbols in ordering scheme and partitionBy
 * to avoid data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughWindow
        implements Rule<ProjectNode>
{
    private static final Capture<WindowNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughWindow(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(window().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        WindowNode windowNode = captures.get(CHILD);
        Map<DereferenceExpression, Symbol> dereferenceProjections = validDereferences(
                ImmutableList.<Expression>builder()
                    .addAll(projectNode.getAssignments().getExpressions())
                    // also include dereference projections used in window functions
                    .addAll(windowNode.getWindowFunctions().values().stream()
                            .flatMap(function -> function.getArguments().stream())
                            .collect(toImmutableList()))
                    .build(),
                context,
                typeAnalyzer,
                true);

        WindowNode.Specification specification = windowNode.getSpecification();
        Map<DereferenceExpression, Symbol> pushdownDereferences = dereferenceProjections.entrySet().stream()
                .filter(entry -> {
                    Symbol symbol = getBase(entry.getKey());
                    // Exclude partitionBy, orderBy and synthesized symbols
                    return !specification.getPartitionBy().contains(symbol) &&
                            !specification.getOrderingScheme().map(OrderingScheme::getOrderBy).orElse(ImmutableList.of()).contains(symbol) &&
                            !windowNode.getCreatedSymbols().contains(symbol);
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        Map<DereferenceExpression, SymbolReference> expressionMapping = pushdownDereferences.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()));

        Assignments newAssignments = projectNode.getAssignments().rewrite(new ExpressionNodeInliner(expressionMapping));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new WindowNode(
                                windowNode.getId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        windowNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(windowNode.getSource().getOutputSymbols())
                                                .putAll(HashBiMap.create(pushdownDereferences).inverse())
                                                .build()),
                                windowNode.getSpecification(),
                                // Replace dereference expressions in functions
                                windowNode.getWindowFunctions().entrySet().stream()
                                        .collect(toImmutableMap(
                                                Map.Entry::getKey,
                                                entry -> {
                                                    WindowNode.Function oldFunction = entry.getValue();
                                                    return new WindowNode.Function(
                                                            oldFunction.getResolvedFunction(),
                                                            oldFunction.getArguments().stream()
                                                                    .map(expression -> replaceExpression(expression, expressionMapping))
                                                                    .collect(toImmutableList()),
                                                            oldFunction.getFrame(),
                                                            oldFunction.isIgnoreNulls());
                                                })),
                                windowNode.getHashSymbol(),
                                windowNode.getPrePartitionedInputs(),
                                windowNode.getPreSortedOrderPrefix()),
                        newAssignments));
    }
}
