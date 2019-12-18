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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.OutputStreamSliceOutput;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.ReaderPageSourceWithProjections;
import io.prestosql.plugin.hive.ReaderRecordCursorWithProjections;
import io.prestosql.plugin.hive.RecordFileWriter;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.benchmark.BenchmarkHiveFileFormatUtil.TestData;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.rcfile.AircompressorCodecFactory;
import io.prestosql.rcfile.HadoopCodecFactory;
import io.prestosql.rcfile.RcFileEncoding;
import io.prestosql.rcfile.RcFileWriter;
import io.prestosql.rcfile.binary.BinaryRcFileEncoding;
import io.prestosql.rcfile.text.TextRcFileEncoding;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.createGenericHiveRecordCursorProvider;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public enum FileFormat
{
    PRESTO_RCBINARY {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSourceForBaseColumns(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCBINARY);
        }

        @Override
        public Optional<ReaderPageSourceWithProjections> createReaderPageSourceWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumnNames,
                List<Type> writeColumnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            ReaderPageSourceWithProjections delegate = createPageSourceWithProjections(pageSourceFactory, session, targetFile, readColumns, writeColumnNames, writeColumnTypes, HiveStorageFormat.RCBINARY);
            return Optional.of(delegate);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new BinaryRcFileEncoding(),
                    compressionCodec);
        }
    },

    PRESTO_RCTEXT {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSourceForBaseColumns(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCTEXT);
        }

        @Override
        public Optional<ReaderPageSourceWithProjections> createReaderPageSourceWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumnNames,
                List<Type> writeColumnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            ReaderPageSourceWithProjections delegate = createPageSourceWithProjections(pageSourceFactory, session, targetFile, readColumns, writeColumnNames, writeColumnTypes, HiveStorageFormat.RCTEXT);
            return Optional.of(delegate);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new TextRcFileEncoding(DateTimeZone.forID(session.getTimeZoneKey().getId())),
                    compressionCodec);
        }
    },

    PRESTO_ORC {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new OrcPageSourceFactory(false, new OrcReaderOptions(), hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSourceForBaseColumns(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.ORC);
        }

        @Override
        public Optional<ReaderPageSourceWithProjections> createReaderPageSourceWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumnNames,
                List<Type> writeColumnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new OrcPageSourceFactory(false, new OrcReaderOptions(), hdfsEnvironment, new FileFormatDataSourceStats());
            ReaderPageSourceWithProjections delegate = createPageSourceWithProjections(pageSourceFactory, session, targetFile, readColumns, writeColumnNames, writeColumnTypes, HiveStorageFormat.ORC);
            return Optional.of(delegate);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoOrcFormatWriter(
                    targetFile,
                    columnNames,
                    columnTypes,
                    DateTimeZone.forID(session.getTimeZoneKey().getId()),
                    compressionCodec);
        }
    },

    PRESTO_PARQUET {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new ParquetReaderConfig());
            return createPageSourceForBaseColumns(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.PARQUET);
        }

        @Override
        public Optional<ReaderPageSourceWithProjections> createReaderPageSourceWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumnNames,
                List<Type> writeColumnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new ParquetReaderConfig());
            ReaderPageSourceWithProjections delegate = createPageSourceWithProjections(pageSourceFactory, session, targetFile, readColumns, writeColumnNames, writeColumnTypes, HiveStorageFormat.PARQUET);
            return Optional.of(delegate);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET, session);
        }
    },

    HIVE_RCBINARY {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSourceForBaseColumns(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCBINARY);
        }

        @Override
        public Optional<ReaderRecordCursorWithProjections> createReaderRecordCursorWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumns,
                List<Type> writeTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return Optional.of(createRecordCursorWithProjections(cursorProvider, session, targetFile, readColumns, writeColumns, writeTypes, HiveStorageFormat.RCBINARY));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCBINARY, session);
        }
    },

    HIVE_RCTEXT {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSourceForBaseColumns(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCTEXT);
        }

        @Override
        public Optional<ReaderRecordCursorWithProjections> createReaderRecordCursorWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumns,
                List<Type> writeTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return Optional.of(createRecordCursorWithProjections(cursorProvider, session, targetFile, readColumns, writeColumns, writeTypes, HiveStorageFormat.RCTEXT));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCTEXT, session);
        }
    },

    HIVE_ORC {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSourceForBaseColumns(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.ORC);
        }

        @Override
        public Optional<ReaderRecordCursorWithProjections> createReaderRecordCursorWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumns,
                List<Type> writeTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return Optional.of(createRecordCursorWithProjections(cursorProvider, session, targetFile, readColumns, writeColumns, writeTypes, HiveStorageFormat.ORC));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.ORC, session);
        }
    },

    HIVE_PARQUET {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new ParquetReaderConfig());
            return createPageSourceForBaseColumns(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.PARQUET);
        }

        @Override
        public Optional<ReaderRecordCursorWithProjections> createReaderRecordCursorWithProjections(
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                File targetFile,
                List<HiveColumnHandle> readColumns,
                List<String> writeColumns,
                List<Type> writeTypes)
        {
            HiveRecordCursorProvider cursorProvider = createGenericHiveRecordCursorProvider(hdfsEnvironment);
            return Optional.of(createRecordCursorWithProjections(cursorProvider, session, targetFile, readColumns, writeColumns, writeTypes, HiveStorageFormat.PARQUET));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET, session);
        }
    };

    public boolean supportsDate()
    {
        return true;
    }

    public abstract ConnectorPageSource createFileFormatReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes);

    public abstract FormatWriter createFileFormatWriter(
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveCompressionCodec compressionCodec)
            throws IOException;

    public Optional<ReaderPageSourceWithProjections> createReaderPageSourceWithProjections(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<HiveColumnHandle> readColumns,
            List<String> writeColumns,
            List<Type> writeTypes)
    {
        return Optional.empty();
    }

    public Optional<ReaderRecordCursorWithProjections> createReaderRecordCursorWithProjections(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<HiveColumnHandle> readColumns,
            List<String> writeColumns,
            List<Type> writeTypes)
    {
        return Optional.empty();
    }

    private static final JobConf conf;

    static {
        conf = new JobConf(new Configuration(false));
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    public boolean supports(TestData testData)
    {
        return true;
    }

    private static ConnectorPageSource createPageSourceForBaseColumns(
            HiveRecordCursorProvider cursorProvider,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            columnHandles.add(createBaseColumn(columnName, i, toHiveType(typeTranslator, columnType), columnType, REGULAR, Optional.empty()));
        }

        ReaderRecordCursorWithProjections recordCursorWithProjections = createRecordCursorWithProjections(cursorProvider, session, targetFile, columnHandles, columnNames, columnTypes, format);

        checkState(!recordCursorWithProjections.getProjectedReaderColumns().isPresent(), "projections should not be required for base columns");

        return new RecordPageSource(columnTypes, recordCursorWithProjections.getRecordCursor());
    }

    private static ReaderRecordCursorWithProjections createRecordCursorWithProjections(
            HiveRecordCursorProvider cursorProvider,
            ConnectorSession session,
            File targetFile,
            List<HiveColumnHandle> readColumns,
            List<String> writeColumnNames,
            List<Type> writeColumnTypes,
            HiveStorageFormat format)
    {
        Optional<ReaderRecordCursorWithProjections> recordCursorWithProjections = cursorProvider.createRecordCursor(
                conf,
                session,
                new Path(targetFile.getAbsolutePath()),
                0,
                targetFile.length(),
                targetFile.length(),
                createSchema(format, writeColumnNames, writeColumnTypes),
                readColumns,
                TupleDomain.all(),
                DateTimeZone.forID(session.getTimeZoneKey().getId()),
                TYPE_MANAGER,
                false);

        return recordCursorWithProjections.get();
    }

    private static ConnectorPageSource createPageSourceForBaseColumns(
            HivePageSourceFactory pageSourceFactory,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            columnHandles.add(createBaseColumn(columnName, i, toHiveType(typeTranslator, columnType), columnType, REGULAR, Optional.empty()));
        }

        ReaderPageSourceWithProjections readerPageSourceWithProjections = createPageSourceWithProjections(pageSourceFactory, session, targetFile, columnHandles, columnNames, columnTypes, format);

        checkState(!readerPageSourceWithProjections.getProjectedReaderColumns().isPresent(), "projection should not be required for base columns");

        return readerPageSourceWithProjections.getConnectorPageSource();
    }

    private static ReaderPageSourceWithProjections createPageSourceWithProjections(
            HivePageSourceFactory pageSourceFactory,
            ConnectorSession session,
            File targetFile,
            List<HiveColumnHandle> readColumns,
            List<String> writeColumnNames,
            List<Type> writeColumnTypes,
            HiveStorageFormat format)
    {
        Properties schema = createSchema(format, writeColumnNames, writeColumnTypes);
        Optional<ReaderPageSourceWithProjections> readerPageSourceWithProjections = pageSourceFactory
                .createPageSource(
                        conf,
                        session,
                        new Path(targetFile.getAbsolutePath()),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        schema,
                        readColumns,
                        TupleDomain.all(),
                        DateTimeZone.forID(session.getTimeZoneKey().getId()));

        return readerPageSourceWithProjections.get();
    }

    private static class RecordFormatWriter
            implements FormatWriter
    {
        private final RecordFileWriter recordWriter;

        public RecordFormatWriter(
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec,
                HiveStorageFormat format,
                ConnectorSession session)
        {
            JobConf config = new JobConf(conf);
            configureCompression(config, compressionCodec);

            recordWriter = new RecordFileWriter(
                    new Path(targetFile.toURI()),
                    columnNames,
                    fromHiveStorageFormat(format),
                    createSchema(format, columnNames, columnTypes),
                    format.getEstimatedWriterSystemMemoryUsage(),
                    config,
                    TYPE_MANAGER,
                    session);
        }

        @Override
        public void writePage(Page page)
        {
            for (int position = 0; position < page.getPositionCount(); position++) {
                recordWriter.appendRow(page, position);
            }
        }

        @Override
        public void close()
        {
            recordWriter.commit();
        }
    }

    public static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, columnNames.stream()
                .collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(type -> toHiveType(typeTranslator, type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }

    private static class PrestoRcFileFormatWriter
            implements FormatWriter
    {
        private final RcFileWriter writer;

        public PrestoRcFileFormatWriter(File targetFile, List<Type> types, RcFileEncoding encoding, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new RcFileWriter(
                    new OutputStreamSliceOutput(new FileOutputStream(targetFile)),
                    types,
                    encoding,
                    compressionCodec.getCodec().map(Class::getName),
                    new AircompressorCodecFactory(new HadoopCodecFactory(getClass().getClassLoader())),
                    ImmutableMap.of(),
                    true);
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private static class PrestoOrcFormatWriter
            implements FormatWriter
    {
        private final OrcWriter writer;

        public PrestoOrcFormatWriter(File targetFile, List<String> columnNames, List<Type> types, DateTimeZone hiveStorageTimeZone, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new OrcWriter(
                    new OutputStreamOrcDataSink(new FileOutputStream(targetFile)),
                    columnNames,
                    types,
                    compressionCodec.getOrcCompressionKind(),
                    new OrcWriterOptions(),
                    false,
                    ImmutableMap.of(),
                    hiveStorageTimeZone,
                    false,
                    BOTH,
                    new OrcWriterStats());
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }
}
