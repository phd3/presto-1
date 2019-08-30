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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(a_x := a.x)
 *      Unnest(replicate = [a], unnest = (b_array -> [b_bigint]))
 *          Source(a, b_array)
 *  </pre>
 * to:
 * <pre>
 *  Project(a_x := symbol)
 *      Unnest(replicate = [a, symbol], unnest = (b_array -> [b_bigint]))
 *          Project(a := a, symbol := a.x, b_array := b_array)
 *              Source(a, b_array)
 * </pre>
 *
 * Pushes down dereference projections through Unnest. Currently, the pushdown is only supported for dereferences on replicate symbols.
 */
public class PushDownDereferenceThroughUnnest
        implements Rule<ProjectNode>
{
    private static final Capture<UnnestNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughUnnest(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(unnest().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        UnnestNode unnestNode = captures.get(CHILD);

        // Extract dereferences from project node's assignments and unnest node's filter
        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        expressionsBuilder.addAll(projectNode.getAssignments().getExpressions());
        unnestNode.getFilter().ifPresent(expressionsBuilder::add);

        Map<DereferenceExpression, Symbol> dereferences = validDereferences(expressionsBuilder.build(), context, typeAnalyzer, true);

        // Pushdown dereferences on replicate symbols
        Map<DereferenceExpression, Symbol> pushdownDereferences = dereferences.entrySet().stream()
                .filter(entry -> unnestNode.getReplicateSymbols().contains(getBase(entry.getKey())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        // Create a new ProjectNode (above the original source) adding dereference projections on replicated symbols
        ProjectNode source = new ProjectNode(
                context.getIdAllocator().getNextId(),
                unnestNode.getSource(),
                Assignments.builder()
                        .putIdentities(unnestNode.getSource().getOutputSymbols())
                        .putAll(HashBiMap.create(pushdownDereferences).inverse())
                        .build());

        Map<DereferenceExpression, SymbolReference> expressionMapping = pushdownDereferences.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()));

        // Modify replicate symbols and filter
        UnnestNode newUnnest = new UnnestNode(
                context.getIdAllocator().getNextId(),
                source,
                ImmutableList.<Symbol>builder()
                    .addAll(unnestNode.getReplicateSymbols())
                    .addAll(pushdownDereferences.values())
                    .build(),
                unnestNode.getMappings(),
                unnestNode.getOrdinalitySymbol(),
                unnestNode.getJoinType(),
                unnestNode.getFilter().map(filter -> replaceExpression(filter, expressionMapping)));

        // Create projectNode with the new unnest node and assignments with replaced dereferences
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        newUnnest,
                        projectNode.getAssignments().rewrite(new ExpressionNodeInliner(expressionMapping))));
    }
}
