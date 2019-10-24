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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class HiveApplyProjectionHelper
{
    private HiveApplyProjectionHelper(){}

    public static List<ConnectorExpression> getSupportedSubExpressions(ConnectorExpression expression)
    {
        ImmutableList.Builder<ConnectorExpression> supportedSubExpressions = ImmutableList.builder();
        fillSupportedSubExpressions(expression, supportedSubExpressions);
        return supportedSubExpressions.build();
    }

    private static void fillSupportedSubExpressions(ConnectorExpression expression, ImmutableList.Builder<ConnectorExpression> supportedSubExpressions)
    {
        if (isSupported(expression)) {
            supportedSubExpressions.add(expression);
            return;
        }

        if (expression instanceof FieldDereference) {
            fillSupportedSubExpressions(((FieldDereference) expression).getTarget(), supportedSubExpressions);
        }
    }

    private static boolean isSupported(ConnectorExpression expression)
    {
        if (expression instanceof Variable) {
            return true;
        }

        if (isSimpleDereferenceChain(expression)) {
            return true;
        }

        return false;
    }

    static boolean isSimpleDereferenceChain(ConnectorExpression expression)
    {
        if (expression instanceof FieldDereference) {
            ConnectorExpression target = ((FieldDereference) expression).getTarget();

            while (true) {
                if (target instanceof Variable) {
                    return true;
                }
                else if (target instanceof FieldDereference) {
                    target = ((FieldDereference) target).getTarget();
                }
                else {
                    return false;
                }
            }
        }

        return false;
    }

    public static Map<ConnectorExpression, DereferenceInfo> getSupersetSubExpressions(Set<ConnectorExpression> expressions)
    {
        ImmutableMap.Builder<ConnectorExpression, DereferenceInfo> superset = ImmutableMap.builder();

        for (ConnectorExpression expression : expressions) {
            checkArgument(expression instanceof Variable || isSimpleDereferenceChain(expression));

            List<ConnectorExpression> allPrefixes = createOrderedPrefixes(expression);
            List<ConnectorExpression> prefixes = allPrefixes.subList(0, allPrefixes.size() - 1);

            if (!prefixes.stream().anyMatch(expressions::contains)) {
                superset.put(expression, createDereferenceInfo(expression));
            }
        }

        return superset.build();
    }

    private static DereferenceInfo createDereferenceInfo(ConnectorExpression expression)
    {
        List<ConnectorExpression> orderedPrefixes = createOrderedPrefixes(expression);
        checkArgument(orderedPrefixes.size() > 0, "invalid ordered prefixes");

        Variable variable = (Variable) orderedPrefixes.get(0);
        ImmutableList.Builder<Integer> dereferenceIntegers = ImmutableList.builder();

        for (int i = 1; i < orderedPrefixes.size(); i++) {
            FieldDereference dereference = (FieldDereference) orderedPrefixes.get(i);
            dereferenceIntegers.add(dereference.getField());
        }

        return new DereferenceInfo(variable, dereferenceIntegers.build());
    }

    /**
     * Return a list of all prefixes for a variable OR a dereference chain ordered in the increasing order of their
     * lengths. For example, this method would return a list of {@link ConnectorExpression} representing ["a", "a.b", "a.c"]
     * for a {@link ConnectorExpression} "a.b.c".
     */
    static List<ConnectorExpression> createOrderedPrefixes(ConnectorExpression expression)
    {
        ImmutableList.Builder<ConnectorExpression> prefixes = ImmutableList.builder();

        ConnectorExpression current = expression;

        while (true) {
            checkArgument(expression instanceof FieldDereference || expression instanceof Variable);
            prefixes.add(current);

            if (current instanceof Variable) {
                break;
            }
            else {
                current = ((FieldDereference) current).getTarget();
            }
        }

        return prefixes.build().reverse();
    }

    /**
     * Replace all connector expressions with variables as given by {@param expressionToVariableMappings} in a top down manner.
     * i.e. if the replacement occurs for the parent, the children would not be visited.
     */
    public static ConnectorExpression replaceWithNewVariables(ConnectorExpression expression, Map<ConnectorExpression, Variable> expressionToVariableMappings)
    {
        if (expressionToVariableMappings.containsKey(expression)) {
            return expressionToVariableMappings.get(expression);
        }

        if (expression instanceof FieldDereference) {
            ConnectorExpression newTarget = replaceWithNewVariables(((FieldDereference) expression).getTarget(), expressionToVariableMappings);
            return new FieldDereference(expression.getType(), newTarget, ((FieldDereference) expression).getField());
        }

        if (expression instanceof Variable) {
            return expression;
        }

        if (expression instanceof Constant) {
            return expression;
        }

        return expression;
    }

    public static class DereferenceInfo
    {
        private final Variable variable;
        private final List<Integer> dereferenceIndices;

        public DereferenceInfo(Variable variable, List<Integer> dereferenceIndices)
        {
            this.variable = requireNonNull(variable, "variable is null");
            this.dereferenceIndices = ImmutableList.copyOf(requireNonNull(dereferenceIndices, "dereferenceIndices is null"));
        }

        public Variable getVariable()
        {
            return variable;
        }

        public List<Integer> getDereferenceIndices()
        {
            return dereferenceIndices;
        }

        public boolean isVariable()
        {
            return dereferenceIndices.size() == 0;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            DereferenceInfo that = (DereferenceInfo) obj;
            return Objects.equals(variable, that.variable) &&
                    Objects.equals(dereferenceIndices, that.dereferenceIndices);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(variable, dereferenceIndices);
        }
    }
}
