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
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topNRowNumber;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(msg1_x := msg1.x, msg2_x := msg2.x, msg3_x := msg3.x)
 *      TopNRowNumber(partitionBy = [msg2], orderBy = [msg3])
 *          Source(msg1, msg2, msg3)
 *  </pre>
 * to:
 * <pre>
 *  Project(msg1_x := symbol, msg2_x := msg2.x, msg3_x := msg3.x)
 *      TopNRowNumber(partitionBy = [msg2], orderBy = [msg3])
 *          Project(msg1 := msg1, symbol := msg1.x, msg2 := msg2, msg3 := msg3)
 *              Source(msg1, msg2, msg3)
 * </pre>
 *
 * Pushes down dereference projections through TopNRowNumber. Excludes dereferences on symbols in partitionBy and ordering scheme
 * to avoid data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughTopNRowNumber
        implements Rule<ProjectNode>
{
    private static final Capture<TopNRowNumberNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughTopNRowNumber(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(topNRowNumber().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        TopNRowNumberNode topNRowNumberNode = captures.get(CHILD);
        Map<DereferenceExpression, Symbol> dereferenceProjections = validDereferences(projectNode.getAssignments().getExpressions(), context, typeAnalyzer, true);

        // Exclude dereferences on symbols being used in partitionBy and orderBy
        WindowNode.Specification specification = topNRowNumberNode.getSpecification();
        Map<DereferenceExpression, Symbol> pushdownDereferences = dereferenceProjections.entrySet().stream()
                .filter(entry -> {
                    Symbol symbol = getBase(entry.getKey());
                    return !specification.getPartitionBy().contains(symbol)
                            && !specification.getOrderingScheme().map(OrderingScheme::getOrderBy).orElse(ImmutableList.of()).contains(symbol);
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        Assignments newAssignments = projectNode.getAssignments().rewrite(new ExpressionNodeInliner(pushdownDereferences.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()))));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        topNRowNumberNode.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        topNRowNumberNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(topNRowNumberNode.getSource().getOutputSymbols())
                                                .putAll(HashBiMap.create(pushdownDereferences).inverse())
                                                .build()))),
                        newAssignments));
    }
}
