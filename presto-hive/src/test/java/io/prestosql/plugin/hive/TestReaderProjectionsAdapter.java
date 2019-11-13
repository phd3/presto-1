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
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.block.BlockAssertions.assertBlockEquals;
import static io.prestosql.plugin.hive.ReaderProjections.projectBaseColumns;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createPartialColumnHandle;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createTestFullColumns;
import static io.prestosql.plugin.hive.TestReaderProjectionsAdapter.RowData.rowData;
import static io.prestosql.spi.block.RowBlock.fromFieldBlocks;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class TestReaderProjectionsAdapter
{
    private static final List<String> TEST_COLUMN_NAMES = ImmutableList.of("col_struct_of_non_primitives");
    private static final Map<String, Type> TEST_COLUMN_TYPES = ImmutableMap.of("col_struct_of_non_primitives", ROWTYPE_OF_ROW_AND_PRIMITIVES);

    private static final Map<String, HiveColumnHandle> TEST_FULL_COLUMNS = createTestFullColumns(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES);

    @Test
    public void testAdaptPage()
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createPartialColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0, 0)),
                createPartialColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0)));

        Optional<ReaderProjections> readerProjections = projectBaseColumns(columns);

        List<Object> inputBlockData = new ArrayList<>();
        inputBlockData.add(rowData(rowData(11L, 12L, 13L), 1L));
        inputBlockData.add(rowData(null, 2L));
        inputBlockData.add(null);
        inputBlockData.add(rowData(rowData(31L, 32L, 33L), 3L));

        ReaderProjectionsAdapter adapter = new ReaderProjectionsAdapter(columns, readerProjections.get());
        verifyPageAdaptation(adapter, ImmutableList.of(inputBlockData));
    }

    private void verifyPageAdaptation(ReaderProjectionsAdapter adapter, List<List<Object>> inputPageData)
    {
        List<ReaderProjectionsAdapter.ChannelMapping> columnMapping = adapter.getOutputToInputMapping();
        List<Type> outputTypes = adapter.getOutputTypes();
        List<Type> inputTypes = adapter.getInputTypes();

        // Create an input page from inputPageData
        Block[] inputPageBlocks = new Block[inputPageData.size()];
        for (int i = 0; i < inputPageBlocks.length; i++) {
            Type inputColumnType = inputTypes.get(i);
            inputPageBlocks[i] = createInputBlock(inputPageData.get(i), inputColumnType);
        }

        Page inputPage = new Page(inputPageBlocks);
        Page outputPage = adapter.adaptPage(inputPage).getLoadedPage();

        // Verify output block values
        for (int i = 0; i < columnMapping.size(); i++) {
            ReaderProjectionsAdapter.ChannelMapping mapping = columnMapping.get(i);
            int inputBlockIndex = mapping.getInputChannelIndex();
            verifyBlock(
                    outputPage.getBlock(i),
                    outputTypes.get(i),
                    inputPage.getBlock(inputBlockIndex),
                    inputTypes.get(inputBlockIndex),
                    mapping.getDereferenceSequence());
        }
    }

    private static Block createInputBlock(List<Object> data, Type type)
    {
        int positionCount = data.size();

        if (type instanceof RowType) {
            return new LazyBlock(data.size(), () -> createRowBlockWithLazyNestedBlocks(data, (RowType) type));
        }
        else if (BIGINT.equals(type)) {
            return new LazyBlock(positionCount, () -> createLongArrayBlock(data));
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    private static Block createRowBlockWithLazyNestedBlocks(List<Object> data, RowType rowType)
    {
        int positionCount = data.size();

        boolean[] isNull = new boolean[positionCount];
        int fieldCount = rowType.getFields().size();

        List<List<Object>> fieldsData = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fieldsData.add(new ArrayList<>());
        }

        // Extract data to generate fieldBlocks
        for (int position = 0; position < data.size(); position++) {
            RowData row = (RowData) data.get(position);
            if (row == null) {
                isNull[position] = true;
            }
            else {
                for (int field = 0; field < fieldCount; field++) {
                    fieldsData.get(field).add(row.getField(field));
                }
            }
        }

        Block[] fieldBlocks = new Block[fieldCount];
        for (int field = 0; field < fieldCount; field++) {
            fieldBlocks[field] = createInputBlock(fieldsData.get(field), rowType.getFields().get(field).getType());
        }

        return fromFieldBlocks(positionCount, Optional.of(isNull), fieldBlocks);
    }

    private static Block createLongArrayBlock(List<Object> data)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, data.size());
        for (int i = 0; i < data.size(); i++) {
            Long value = (Long) data.get(i);
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeLong(value);
            }
        }
        return builder.build();
    }

    private static void verifyBlock(Block actualBlock, Type outputType, Block input, Type inputType, List<Integer> dereferences)
    {
        Block expectedOutputBlock = createPartialColumnBlock(input, outputType, inputType, dereferences);
        assertBlockEquals(outputType, actualBlock, expectedOutputBlock);
    }

    private static Block createPartialColumnBlock(Block data, Type finalType, Type blockType, List<Integer> dereferences)
    {
        if (dereferences.size() == 0) {
            return data;
        }

        BlockBuilder builder = finalType.createBlockBuilder(null, data.getPositionCount());

        for (int i = 0; i < data.getPositionCount(); i++) {
            Type sourceType = blockType;
            Block currentData = data.getObject(i, Block.class);
            boolean isNull = (currentData == null);

            for (int j = 0; j < dereferences.size() - 1; j++) {
                if (isNull) {
                    break;
                }

                checkArgument(sourceType instanceof RowType);
                currentData = currentData.getObject(dereferences.get(j), Block.class);
                sourceType = ((RowType) sourceType).getFields().get(dereferences.get(j)).getType();
                isNull = isNull || (currentData == null);
            }

            if (isNull) {
                builder.appendNull();
            }
            else {
                int lastDereference = dereferences.get(dereferences.size() - 1);

                if (finalType.equals(BIGINT)) {
                    Long value = currentData.getLong(lastDereference, 0);
                    if (value == null) {
                        builder.appendNull();
                    }
                    else {
                        builder.writeLong(value);
                    }
                }
                else if (finalType instanceof RowType) {
                    Block block = currentData.getObject(lastDereference, Block.class);
                    if (block == null) {
                        builder.appendNull();
                    }
                    else {
                        builder.appendStructure(block);
                    }
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }
        }

        return builder.build();
    }

    static class RowData
    {
        private final List<? extends Object> data;

        private RowData(Object... data)
        {
            this.data = requireNonNull(Arrays.asList(data), "data is null");
        }

        static RowData rowData(Object... data)
        {
            return new RowData(data);
        }

        List<? extends Object> getData()
        {
            return data;
        }

        Object getField(int field)
        {
            checkArgument(field >= 0 && field < data.size());
            return data.get(field);
        }
    }
}
