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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.ExpressionNodeInliner;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(msg_x := msg.x)
 *    Project(msg := msg)
 *      Source(msg)
 *  </pre>
 * to:
 * <pre>
 *  Project(msg_x := symbol)
 *    Project(msg := msg, symbol := msg.x)
 *      Source(msg)
 * </pre>
 */
public class PushDownDereferenceThroughProject
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughProject(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(project().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        // Extract dereferences from assignments for pushdown
        Map<DereferenceExpression, Symbol> dereferences = validDereferences(node.getAssignments().getExpressions(), context, typeAnalyzer, true).entrySet().stream()
                .filter(entry -> child.getSource().getOutputSymbols().contains(getBase(entry.getKey()))) // exclude dereferences on symbols being synthesized within child
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (dereferences.isEmpty()) {
            return Result.empty();
        }

        // Prepare new assignments replacing dereferences with new symbols
        Assignments assignments = node.getAssignments().rewrite(new ExpressionNodeInliner(dereferences.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference()))));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                child.getSource(),
                                Assignments.builder()
                                        .putAll(child.getAssignments())
                                        .putAll(HashBiMap.create(dereferences).inverse())
                                        .build()),
                        assignments));
    }
}
