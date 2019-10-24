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
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.hive.HiveApplyProjectionHelper.createOrderedPrefixes;
import static io.prestosql.plugin.hive.HiveApplyProjectionHelper.getSupersetSubExpressions;
import static io.prestosql.plugin.hive.HiveApplyProjectionHelper.isSimpleDereferenceChain;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveApplyProjectionHelper
{
    private static final ConnectorExpression ROW_OF_ROW_VARIABLE = new Variable("a", rowType(field("a", rowType(field("c", INTEGER)))));

    private static final ConnectorExpression ONE_LEVEL_DEREFERENCE = new FieldDereference(
            rowType(field("c", INTEGER)),
            ROW_OF_ROW_VARIABLE,
            0);

    private static final ConnectorExpression TWO_LEVEL_DEREFERENCE = new FieldDereference(
            INTEGER,
            ONE_LEVEL_DEREFERENCE,
            0);

    private static final ConnectorExpression INT_VARIABLE = new Variable("a", INTEGER);
    private static final ConnectorExpression CONSTANT = new Constant(5, INTEGER);

    @Test
    public void testIsSimpleDereferenceChain()
    {
        assertTrue(isSimpleDereferenceChain(ONE_LEVEL_DEREFERENCE));
        assertTrue(isSimpleDereferenceChain(TWO_LEVEL_DEREFERENCE));
        assertFalse(isSimpleDereferenceChain(INT_VARIABLE));
        assertFalse(isSimpleDereferenceChain(CONSTANT));
    }

    @Test
    public void testCreateOrderedPrefixes()
    {
        assertEquals(getOnlyElement(createOrderedPrefixes(INT_VARIABLE)), INT_VARIABLE);

        List<ConnectorExpression> prefixes = createOrderedPrefixes(ONE_LEVEL_DEREFERENCE);
        assertEquals(prefixes.size(), 2);
        assertEquals(prefixes.get(0), ((FieldDereference) ONE_LEVEL_DEREFERENCE).getTarget());
        assertEquals(prefixes.get(1), ONE_LEVEL_DEREFERENCE);

        prefixes = createOrderedPrefixes(TWO_LEVEL_DEREFERENCE);
        assertEquals(prefixes.size(), 3);
        assertEquals(prefixes.get(0), ((FieldDereference) ((FieldDereference) TWO_LEVEL_DEREFERENCE).getTarget()).getTarget());
        assertEquals(prefixes.get(1), ((FieldDereference) TWO_LEVEL_DEREFERENCE).getTarget());
        assertEquals(prefixes.get(2), TWO_LEVEL_DEREFERENCE);
    }

    @Test
    public void testGetSupersetSubExpressions()
    {
        Map<ConnectorExpression, HiveApplyProjectionHelper.DereferenceInfo> superset = getSupersetSubExpressions(ImmutableSet.of(INT_VARIABLE, TWO_LEVEL_DEREFERENCE));
        assertEquals(superset.keySet(), ImmutableSet.of(INT_VARIABLE, TWO_LEVEL_DEREFERENCE));
        assertEquals(superset.get(INT_VARIABLE), new HiveApplyProjectionHelper.DereferenceInfo(((Variable) INT_VARIABLE), ImmutableList.of()));
        assertEquals(superset.get(TWO_LEVEL_DEREFERENCE), new HiveApplyProjectionHelper.DereferenceInfo(((Variable) ROW_OF_ROW_VARIABLE), ImmutableList.of(0, 0)));

        superset = getSupersetSubExpressions(ImmutableSet.of(ONE_LEVEL_DEREFERENCE, TWO_LEVEL_DEREFERENCE, ROW_OF_ROW_VARIABLE));
        assertEquals(superset.keySet(), ImmutableSet.of(ROW_OF_ROW_VARIABLE));
        assertEquals(superset.get(ROW_OF_ROW_VARIABLE), new HiveApplyProjectionHelper.DereferenceInfo(((Variable) ROW_OF_ROW_VARIABLE), ImmutableList.of()));
    }
}
