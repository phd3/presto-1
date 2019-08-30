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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.dereference.DereferencePushdown.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer extracts all dereference expressions from a filter node located above a table scan into a ProjectNode.
 *
 * Extracting dereferences from a filter (eg. FilterNode(a.x = 5)) can be suboptimal if full columns are being accessed up the
 * plan tree (eg. a), because it can result in replicated shuffling of fields (eg. a.x). So it is safer to pushdown dereferences from
 * Filter only when there's an explicit projection on top of the filter node (Ref PushDereferencesThroughFilter).
 *
 * In case of a FilterNode on top of TableScanNode, we want to push all dereferences into a new ProjectNode below, so that
 * PushProjectionIntoTableScan optimizer can push those columns in the connector, and provide new column handles for the
 * projected subcolumns. PushPredicateIntoTableScan optimizer can then push predicates on these subcolumns into the connector.
 */
public class ExtractDereferencesFromFilterAboveScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public ExtractDereferencesFromFilterAboveScan(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return filter()
                .with(source().matching(tableScan().capturedAs(CHILD)));
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        BiMap<DereferenceExpression, Symbol> expressions =
                HashBiMap.create(validDereferences(ImmutableList.of(node.getPredicate()), context, typeAnalyzer, false));

        if (expressions.isEmpty()) {
            return Result.empty();
        }

        PlanNode source = node.getSource();
        Assignments assignments = Assignments.builder()
                .putIdentities(source.getOutputSymbols())
                .putAll(expressions.inverse())
                .build();

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                new FilterNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(context.getIdAllocator().getNextId(), source, assignments),
                        replaceExpression(
                                node.getPredicate(),
                                expressions.entrySet().stream()
                                        .collect(toImmutableMap(Map.Entry::getKey, mapping -> mapping.getValue().toSymbolReference())))),
                Assignments.identity(node.getOutputSymbols())));
    }
}
