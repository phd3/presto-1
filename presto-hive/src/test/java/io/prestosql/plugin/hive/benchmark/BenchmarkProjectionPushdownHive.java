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
package io.prestosql.plugin.hive.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.hive.HiveReaderProjectionsAdaptingRecordCursor;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.ReaderPageSourceWithProjections;
import io.prestosql.plugin.hive.ReaderProjections;
import io.prestosql.plugin.hive.ReaderProjectionsAdapter;
import io.prestosql.plugin.hive.ReaderRecordCursorWithProjections;
import io.prestosql.plugin.hive.benchmark.BenchmarkHiveFileFormatUtil.TestData;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.operator.OperatorAssertion.toRow;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.prestosql.plugin.hive.benchmark.BenchmarkHiveFileFormatUtil.MIN_DATA_SIZE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.nio.file.Files.createTempDirectory;
import static java.util.stream.Collectors.toList;

/**
 * Benchmarks the read operations with projection pushdown in Hive. This is useful for comparing the following for different formats and compressions
 * - Performance difference between reading row columns v/s reading projected VARCHAR subfields
 * - Performance difference between reading base VARCHAR columns v/s reading projected VARCHAR subfields
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 50)
@Warmup(iterations = 20)
@Fork(3)
public class BenchmarkProjectionPushdownHive
{
    private static final String FLATTENED = "flattenned";
    private static final String UNFLATTENED_WITH_PUSHDOWN = "unflattened_with_pushdown";
    private static final String UNFLATTENED_WITHOUT_PUSHDOWN = "unflattened_without_pushdown";

    private static final Type VARCHAR = createUnboundedVarcharType();
    private static final HiveType HIVE_VARCHAR = HiveType.toHiveType(new HiveTypeTranslator(), createUnboundedVarcharType());

    private List<Type> columnTypesToWrite;
    private List<String> columnNamesToWrite;
    private List<HiveType> columnHiveTypesToWrite;

    // Format for writing and reading columns
    // FLATTENED strategy: Write "N" VARCHAR columns and read "M" of them
    // UNFLATTENED_WITHOUT_PUSHDOWN strategy: Write one ROW column with "N" VARCHAR fields and read it
    // UNFLATTENED_WITH_PUSHDOWN: Write one ROW column with "N" VARCHAR fields and read "M" fields from it
    @Param({FLATTENED, UNFLATTENED_WITH_PUSHDOWN, UNFLATTENED_WITHOUT_PUSHDOWN})
    private String strategy;

    // Number of VARCHAR columns/subcolumns to write
    @Param("3")
    private int fieldCount;

    // Number of VARCHAR columns/subcolumns to read
    @Param("1")
    private int fieldsToRead;

    @Param({"NONE", "GZIP", "SNAPPY"})
    private HiveCompressionCodec compression;

    @Param({"PRESTO_ORC", "PRESTO_PARQUET"})
    private FileFormat fileFormat;

    private TestData dataToWrite;
    private File dataFile;

    private final File targetDir = createTempDir("presto-benchmark");

    @Setup
    public void setup()
            throws IOException
    {
        if (FLATTENED.equals(strategy)) {
            // Write VARCHAR columns
            columnTypesToWrite = ImmutableList.copyOf(Collections.nCopies(fieldCount, VARCHAR));
            columnHiveTypesToWrite = ImmutableList.copyOf(Collections.nCopies(fieldCount, HIVE_VARCHAR));
            columnNamesToWrite = IntStream.range(0, fieldCount).boxed()
                    .map(i -> "field" + i)
                    .collect(toImmutableList());
        }
        else if (UNFLATTENED_WITH_PUSHDOWN.equals(strategy) || UNFLATTENED_WITHOUT_PUSHDOWN.equals(strategy)) {
            // Write one ROW column with VARCHAR fields
            List<RowType.Field> fields = IntStream.range(0, fieldCount).boxed()
                    .map(i -> new RowType.Field(Optional.of("field" + i), createUnboundedVarcharType()))
                    .collect(toImmutableList());

            Type rowType = RowType.from(fields);
            columnTypesToWrite = ImmutableList.of(rowType);
            columnHiveTypesToWrite = ImmutableList.of(HiveType.toHiveType(new HiveTypeTranslator(), rowType));
            columnNamesToWrite = ImmutableList.of("row_of_varchars");
        }
        else {
            throw new UnsupportedOperationException();
        }

        dataToWrite = createTestData(columnTypesToWrite, columnNamesToWrite);

        targetDir.mkdirs();
        dataFile = new File(targetDir, UUID.randomUUID().toString());
        writeData(dataFile);
    }

    @Benchmark
    public List<Page> readPages()
    {
        List<Page> pages = new ArrayList<>(100);

        if (FLATTENED.equals(strategy)) {
            // Flattened schema has all VARCHAR type columns, read them as is
            checkState(columnTypesToWrite.stream().allMatch(param -> param instanceof VarcharType), "Only VARCHAR types are allowed");

            List<String> columnNamesToRead = columnNamesToWrite.subList(0, fieldsToRead);
            List<Type> columnTypesToRead = columnTypesToWrite.subList(0, fieldsToRead);

            ConnectorPageSource pageSource = fileFormat.createFileFormatReader(SESSION, HDFS_ENVIRONMENT, dataFile, columnNamesToRead, columnTypesToRead);
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    pages.add(page.getLoadedPage());
                }
            }

            return pages;
        }
        else if (UNFLATTENED_WITHOUT_PUSHDOWN.equals(strategy)) {
            // Unflattened schema has one ROW type column
            checkState(columnHiveTypesToWrite.size() == 1);

            // Read whole column from the page source and apply projection in the operator
            ConnectorPageSource pageSource = fileFormat.createFileFormatReader(SESSION, HDFS_ENVIRONMENT, dataFile, columnNamesToWrite, columnTypesToWrite);

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    pages.add(page.getLoadedPage());
                }
            }

            return pages;
        }
        else if (UNFLATTENED_WITH_PUSHDOWN.equals(strategy)) {
            // Read projected columns from the page source
            HiveColumnHandle baseColumn = HiveColumnHandle.createBaseColumn(
                    columnNamesToWrite.get(0),
                    0,
                    columnHiveTypesToWrite.get(0),
                    columnTypesToWrite.get(0),
                    HiveColumnHandle.ColumnType.REGULAR,
                    Optional.empty());

            List<HiveColumnHandle> hiveColumns = IntStream.range(0, fieldsToRead).boxed()
                    .map(i -> createProjectedColumnHandle(baseColumn, ImmutableList.of(i)))
                    .collect(toImmutableList());

            Optional<ReaderPageSourceWithProjections> pageSourceWithProjections = fileFormat.createReaderPageSourceWithProjections(SESSION, HDFS_ENVIRONMENT, dataFile, hiveColumns, columnNamesToWrite, columnTypesToWrite);
            if (pageSourceWithProjections.isPresent()) {
                ConnectorPageSource pageSource = pageSourceWithProjections.get().getConnectorPageSource();
                Optional<ReaderProjections> projections = pageSourceWithProjections.get().getProjectedReaderColumns();
                checkState(projections.isPresent(), "expected projections to be present when reading projected columns");
                ReaderProjectionsAdapter adapter = new ReaderProjectionsAdapter(hiveColumns, projections.get());

                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    page = adapter.adaptPage(page);
                    if (page != null) {
                        pages.add(page.getLoadedPage());
                    }
                }

                return pages;
            }

            Optional<ReaderRecordCursorWithProjections> cursorWithProjections = fileFormat.createReaderRecordCursorWithProjections(SESSION, HDFS_ENVIRONMENT, dataFile, hiveColumns, columnNamesToWrite, columnTypesToWrite);
            if (cursorWithProjections.isPresent()) {
                RecordCursor cursor = cursorWithProjections.get().getRecordCursor();
                Optional<ReaderProjections> projections = cursorWithProjections.get().getProjectedReaderColumns();
                checkState(projections.isPresent(), "expected projections to be present when reading projected columns");

                ReaderProjectionsAdapter projectionsAdapter = new ReaderProjectionsAdapter(hiveColumns, projections.get());
                cursor = new HiveReaderProjectionsAdaptingRecordCursor(cursor, projectionsAdapter);
                RecordPageSource pageSource = new RecordPageSource(hiveColumns.stream().map(c -> c.getType()).collect(toList()), cursor);

                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    if (page != null) {
                        pages.add(page.getLoadedPage());
                    }
                }

                return pages;
            }

            throw new UnsupportedOperationException();
        }

        throw new UnsupportedOperationException();
    }

    @TearDown
    public void tearDown()
            throws IOException
    {
        deleteRecursively(targetDir.toPath(), ALLOW_INSECURE);
    }

    private void writeData(File targetFile)
            throws IOException
    {
        List<Page> inputPages = dataToWrite.getPages();
        try (FormatWriter formatWriter = fileFormat.createFileFormatWriter(
                SESSION,
                targetFile,
                dataToWrite.getColumnNames(),
                dataToWrite.getColumnTypes(),
                compression)) {
            for (Page page : inputPages) {
                formatWriter.writePage(page);
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Options opt = new OptionsBuilder()
                .include(".*\\." + BenchmarkProjectionPushdownHive.class.getSimpleName() + ".*")
                .jvmArgsAppend("-Xmx4g", "-Xms4g", "-XX:+UseG1GC")
                .build();

        new Runner(opt).run();
    }

    @SuppressWarnings("SameParameterValue")
    private static File createTempDir(String prefix)
    {
        try {
            return createTempDirectory(prefix).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static TestData createTestData(List<Type> columnTypes, List<String> columnNames)
    {
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();

        int dataSize = 0;
        Random random = new Random(1234);

        while (dataSize < MIN_DATA_SIZE) {
            pageBuilder.declarePosition();

            for (int i = 0; i < columnTypes.size(); i++) {
                BlockBuilder builder = pageBuilder.getBlockBuilder(i);
                Type blockType = columnTypes.get(i);

                if (blockType instanceof RowType) {
                    List<Type> parameters = blockType.getTypeParameters();
                    checkState(parameters.stream().allMatch(param -> param instanceof VarcharType),
                            "Only VARCHAR types are allowed in the row");

                    // Append the same random value to all fields
                    Slice slice = Slices.utf8Slice(String.join("", Collections.nCopies(5, "key" + random.nextInt(10_000_000))));
                    Object[] values = blockType.getTypeParameters().stream().map(x -> slice).collect(toList()).toArray();
                    blockType.writeObject(builder, toRow(parameters, values));
                }
                else if (blockType instanceof VarcharType) {
                    Slice slice = Slices.utf8Slice(String.join("", Collections.nCopies(5, "key" + random.nextInt(10_000_000))));
                    blockType.writeSlice(builder, slice);
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }

            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pages.add(page);
                pageBuilder.reset();
                dataSize += page.getSizeInBytes();
            }
        }

        return new TestData(columnNames, columnTypes, pages.build());
    }
}
