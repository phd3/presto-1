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
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(msg_x := msg.x)
 *      Limit
 *          Source(msg)
 *  </pre>
 * to:
 * <pre>
 *  Project(msg_x := symbol)
 *      Limit
 *          Project(msg := msg, symbol := msg.x)
 *              Source(msg)
 * </pre>
 */
public class PushDownDereferencesThroughLimit
        implements Rule<ProjectNode>
{
    private static final Capture<LimitNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughLimit(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(limit().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        LimitNode limitNode = captures.get(CHILD);
        Map<DereferenceExpression, Symbol> pushdownDereferences = validDereferences(projectNode.getAssignments().getExpressions(), context, typeAnalyzer, true);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        // Prepare new assignments by replacing dereference expressions with new symbols
        Assignments newAssignments = projectNode.getAssignments().rewrite(new ExpressionNodeInliner(pushdownDereferences.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()))));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        limitNode.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        limitNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(limitNode.getSource().getOutputSymbols())
                                                .putAll(HashBiMap.create(pushdownDereferences).inverse())
                                                .build()))),
                        newAssignments));
    }
}
