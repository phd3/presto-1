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
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(msg1_x := msg1.x, msg2_x := msg2.x)
 *      SemiJoin(sourceJoinSymbol = msg2, filteringSourceJoinSymbol = msg2_filtering)
 *          Source(msg1, msg2)
 *          FilteringSource(msg2_filtering)
 *  </pre>
 * to:
 * <pre>
 *  Project(msg1_x := symbol, msg2_x := msg2.x)
 *          SemiJoinNode(sourceJoinSymbol = msg2, filteringSourceJoinSymbol = msg2_filtering)
 *              Project(msg1 := msg1, msg2 := msg2, symbol := msg1.x)
 *                  Source(msg1, msg2)
 *              FilteringSource(msg2_filtering)
 * </pre>
 *
 * Pushes down dereference projections through SemiJoinNode. Excludes dereferences on sourceJoinSymbol to avoid
 * data replication, since this symbol cannot be pruned.
 */
public class PushDownDereferenceThroughSemiJoin
        implements Rule<ProjectNode>
{
    private static final Capture<SemiJoinNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughSemiJoin(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(semiJoin().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        SemiJoinNode semiJoinNode = captures.get(CHILD);

        Map<DereferenceExpression, Symbol> dereferenceProjections = validDereferences(projectNode.getAssignments().getExpressions(), context, typeAnalyzer, true);

        // All dereferences can be assumed on the symbols coming from source, since filteringSource output is not propagated,
        // and semiJoinOutput is of type boolean. We exclude pushdown of dereferences on sourceJoinSymbol.
        Map<DereferenceExpression, Symbol> pushdownDereferences = dereferenceProjections.entrySet().stream()
                .filter(entry -> !getBase(entry.getKey()).equals(semiJoinNode.getSourceJoinSymbol()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        PlanNode newSource = new ProjectNode(
                context.getIdAllocator().getNextId(),
                semiJoinNode.getSource(),
                Assignments.builder()
                        .putIdentities(semiJoinNode.getSource().getOutputSymbols())
                        .putAll(HashBiMap.create(pushdownDereferences).inverse())
                        .build());

        PlanNode newSemiJoin = semiJoinNode.replaceChildren(ImmutableList.of(newSource, semiJoinNode.getFilteringSource()));

        Assignments assignments = projectNode.getAssignments().rewrite(new ExpressionNodeInliner(
                pushdownDereferences.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()))));

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newSemiJoin, assignments));
    }
}
