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
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveSplit.BucketConversion;
import io.prestosql.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static io.prestosql.plugin.hive.util.HiveUtil.getPrefilledColumnValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final TypeManager typeManager;

    private final Set<HivePageSourceFactory> pageSourceFactories;

    @Inject
    public HivePageSourceProvider(
            HiveConfig hiveConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            TypeManager typeManager)
    {
        requireNonNull(hiveConfig, "hiveConfig is null");
        this.hiveStorageTimeZone = hiveConfig.getDateTimeZone();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(requireNonNull(cursorProviders, "cursorProviders is null"));
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns)
    {
        return createPageSource(transaction, session, split, table, columns, TupleDomain.all());
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, TupleDomain<ColumnHandle> dynamicFilter)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path);

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                configuration,
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileSize(),
                hiveSplit.getFileModifiedTime(),
                hiveSplit.getSchema(),
                hiveTable.getCompactEffectivePredicate().intersect(dynamicFilter.transform(HiveColumnHandle.class::cast)),
                hiveColumns,
                hiveSplit.getPartitionKeys(),
                hiveStorageTimeZone,
                typeManager,
                hiveSplit.getColumnCoercions(),
                hiveSplit.getBucketConversion(),
                hiveSplit.isS3SelectPushdownEnabled());
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new RuntimeException("Could not find a file reader for split " + hiveSplit);
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt bucketNumber,
            long start,
            long length,
            long fileSize,
            long fileModifiedTime,
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            Map<Integer, HiveType> columnCoercions,
            Optional<BucketConversion> bucketConversion,
            boolean s3SelectPushdownEnabled)
    {
        if (effectivePredicate.isNone()) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                partitionKeys,
                columns,
                bucketConversion.map(BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()),
                columnCoercions,
                path,
                bucketNumber,
                fileSize,
                fileModifiedTime);
        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);

        Optional<BucketAdaptation> bucketAdaptation = createBucketAdaptation(bucketConversion, bucketNumber, regularAndInterimColumnMappings);

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            List<HiveColumnHandle> columnsRequestedFromReader = toColumnHandles(regularAndInterimColumnMappings, true, typeManager);

            Optional<ReaderPageSourceWithProjections> readerWithProjections = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    columnsRequestedFromReader,
                    effectivePredicate,
                    hiveStorageTimeZone);

            if (readerWithProjections.isPresent()) {
                ConnectorPageSource pageSource = readerWithProjections.get().getConnectorPageSource();

                Optional<ReaderProjections> readerProjections = readerWithProjections.get().getProjectedReaderColumns();
                ReaderProjectionsAdapter adapter = null;
                if (readerProjections.isPresent()) {
                    adapter = new ReaderProjectionsAdapter(columnsRequestedFromReader, readerProjections.get());
                }

                return Optional.of(new HivePageSource(
                        columnMappings,
                        bucketAdaptation,
                        Optional.ofNullable(adapter),
                        hiveStorageTimeZone,
                        typeManager,
                        pageSource));
            }
        }

        for (HiveRecordCursorProvider provider : cursorProviders) {
            // GenericHiveRecordCursor will automatically do the coercion without HiveCoercionRecordCursor
            boolean doCoercion = !(provider instanceof GenericHiveRecordCursorProvider);

            List<HiveColumnHandle> columnsRequestedFromReader = toColumnHandles(regularAndInterimColumnMappings, doCoercion, typeManager);
            Optional<ReaderRecordCursorWithProjections> readerWithProjections = provider.createRecordCursor(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    columnsRequestedFromReader,
                    effectivePredicate,
                    hiveStorageTimeZone,
                    typeManager,
                    s3SelectPushdownEnabled);

            if (readerWithProjections.isPresent()) {
                RecordCursor delegate = readerWithProjections.get().getRecordCursor();
                Optional<ReaderProjections> projections = readerWithProjections.get().getProjectedReaderColumns();

                if (projections.isPresent()) {
                    ReaderProjectionsAdapter projectionsAdapter = new ReaderProjectionsAdapter(columnsRequestedFromReader, projections.get());
                    delegate = new HiveReaderProjectionsAdaptingRecordCursor(delegate, projectionsAdapter);
                }

                if (bucketAdaptation.isPresent()) {
                    delegate = new HiveBucketAdapterRecordCursor(
                            bucketAdaptation.get().getBucketColumnIndices(),
                            bucketAdaptation.get().getBucketColumnHiveTypes(),
                            bucketAdaptation.get().getBucketingVersion(),
                            bucketAdaptation.get().getTableBucketCount(),
                            bucketAdaptation.get().getPartitionBucketCount(),
                            bucketAdaptation.get().getBucketToKeep(),
                            typeManager,
                            delegate);
                }

                // Need to wrap RcText and RcBinary into a wrapper, which will do the coercion for mismatch columns
                if (doCoercion) {
                    delegate = new HiveCoercionRecordCursor(regularAndInterimColumnMappings, typeManager, delegate);
                }

                HiveRecordCursor hiveRecordCursor = new HiveRecordCursor(
                        columnMappings,
                        hiveStorageTimeZone,
                        delegate);
                List<Type> columnTypes = columns.stream()
                        .map(HiveColumnHandle::getType)
                        .collect(toList());

                return Optional.of(new RecordPageSource(columnTypes, hiveRecordCursor));
            }
        }

        return Optional.empty();
    }

    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;
        private final Optional<HiveType> baseTypeCoercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), baseTypeCoercionFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(), baseTypeCoercionFrom);
        }

        public static ColumnMapping interim(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.INTERIM, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), baseTypeCoercionFrom);
        }

        public static ColumnMapping empty(HiveColumnHandle hiveColumnHandle)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.EMPTY, hiveColumnHandle, Optional.empty(), OptionalInt.empty(), Optional.empty());
        }

        private ColumnMapping(ColumnMappingKind kind, HiveColumnHandle hiveColumnHandle, Optional<String> prefilledValue, OptionalInt index, Optional<HiveType> baseTypeCoercionFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.index = requireNonNull(index, "index is null");
            this.baseTypeCoercionFrom = requireNonNull(baseTypeCoercionFrom, "coerceFrom is null");
        }

        public ColumnMappingKind getKind()
        {
            return kind;
        }

        public String getPrefilledValue()
        {
            checkState(kind == ColumnMappingKind.PREFILLED);
            return prefilledValue.get();
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getIndex()
        {
            checkState(kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM);
            return index.getAsInt();
        }

        public Optional<HiveType> getBaseTypeCoercionFrom()
        {
            return baseTypeCoercionFrom;
        }

        /**
         * @param columns columns that need to be returned to engine
         * @param requiredInterimColumns columns that are needed for processing, but shouldn't be returned to engine (may overlaps with columns)
         * @param columnCoercions map from hive column index to hive type (for base columns)
         * @param bucketNumber empty if table is not bucketed, a number within [0, # bucket in table) otherwise
         */
        public static List<ColumnMapping> buildColumnMappings(
                List<HivePartitionKey> partitionKeys,
                List<HiveColumnHandle> columns,
                List<HiveColumnHandle> requiredInterimColumns,
                Map<Integer, HiveType> columnCoercions,
                Path path,
                OptionalInt bucketNumber,
                long fileSize,
                long fileModifiedTime)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);

            // Maintain state about hive columns added to the mapping as we iterate (for validation)
            Set<Integer> baseColumnHiveIndices = new HashSet<>();
            Map<Integer, Set<Optional<HiveColumnProjectionInfo>>> projectionsForColumn = new HashMap<>();

            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            int regularIndex = 0;

            for (HiveColumnHandle column : columns) {
                Optional<HiveType> baseTypeCoercionFrom = Optional.ofNullable(columnCoercions.get(column.getBaseHiveColumnIndex()));

                if (column.getColumnType() == REGULAR) {
                    if (column.isBaseColumn()) {
                        baseColumnHiveIndices.add(column.getBaseHiveColumnIndex());
                    }
                    checkArgument(add(projectionsForColumn, column.getBaseHiveColumnIndex(), column.getHivePartialColumnInfo()), "duplicate column in columns list");

                    // Add regular mapping if projection is valid for partition schema, otherwise add an empty mapping
                    if (projectionExistsInPartitionSchema(baseTypeCoercionFrom, column)) {
                        columnMappings.add(regular(column, regularIndex, baseTypeCoercionFrom));
                        regularIndex++;
                    }
                    else {
                        columnMappings.add(empty(column));
                    }
                }
                else {
                    columnMappings.add(prefilled(
                            column,
                            getPrefilledColumnValue(column, partitionKeysByName.get(column.getName()), path, bucketNumber, fileSize, fileModifiedTime),
                            baseTypeCoercionFrom));
                }
            }

            for (HiveColumnHandle column : requiredInterimColumns) {
                checkArgument(column.getColumnType() == REGULAR);
                checkArgument(column.isBaseColumn(), "bucketed columns should be base columns");
                if (baseColumnHiveIndices.contains(column.getBaseHiveColumnIndex())) {
                    continue; // This column exists in columns. Do not add it again.
                }

                if (projectionsForColumn.containsKey(column.getBaseHiveColumnIndex())) {
                    columnMappings.add(interim(column, regularIndex, Optional.ofNullable(columnCoercions.get(column.getBaseHiveColumnIndex()))));
                }
                else {
                    // If coercion does not affect bucket number calculation, coercion doesn't need to be applied here.
                    // Otherwise, read of this partition should not be allowed.
                    // (Alternatively, the partition could be read as an unbucketed partition. This is not implemented.)
                    columnMappings.add(interim(column, regularIndex, Optional.empty()));
                }
                regularIndex++;
            }
            return columnMappings.build();
        }

        private static boolean projectionExistsInPartitionSchema(Optional<HiveType> baseTypeCoercionFrom, HiveColumnHandle column)
        {
            if (!baseTypeCoercionFrom.isPresent()) {
                return true;
            }

            HiveType partitionType = baseTypeCoercionFrom.get();
            List<Integer> dereferences = column.getHivePartialColumnInfo()
                    .map(partialColumn -> partialColumn.getDereferenceIndices())
                    .orElse(ImmutableList.of());

            Optional<HiveType> delegateType = partitionType.getHiveTypeForDereferences(dereferences);

            return delegateType.isPresent();
        }

        private static boolean add(Map<Integer, Set<Optional<HiveColumnProjectionInfo>>> subColumns, int baseIndex, Optional<HiveColumnProjectionInfo> dereferences)
        {
            if (subColumns.get(baseIndex) == null) {
                subColumns.put(baseIndex, new HashSet<>());
            }

            return subColumns.get(baseIndex).add(dereferences);
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM)
                    .collect(toImmutableList());
        }

        public static List<HiveColumnHandle> toColumnHandles(List<ColumnMapping> regularColumnMappings, boolean doCoercion, TypeManager typeManager)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        if (!doCoercion || !columnMapping.getBaseTypeCoercionFrom().isPresent()) {
                            return columnHandle;
                        }
                        HiveType fromHiveTypeBase = columnMapping.getBaseTypeCoercionFrom().get();

                        Optional<HiveColumnProjectionInfo> newPartialColumnInfo = columnHandle.getHivePartialColumnInfo().map(partialColumn -> {
                            HiveType fromHiveType = fromHiveTypeBase.getHiveTypeForDereferences(partialColumn.getDereferenceIndices()).get();
                            return new HiveColumnProjectionInfo(
                                    partialColumn.getDereferenceIndices(),
                                    partialColumn.getDereferenceNames(),
                                    fromHiveType,
                                    fromHiveType.getType(typeManager));
                        });

                        return new HiveColumnHandle(
                                columnHandle.getBaseColumnName(),
                                columnHandle.getBaseHiveColumnIndex(),
                                fromHiveTypeBase,
                                fromHiveTypeBase.getType(typeManager),
                                newPartialColumnInfo,
                                columnHandle.getColumnType(),
                                columnHandle.getComment());
                    })
                    .collect(toList());
        }
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
        INTERIM,
        EMPTY
    }

    private static Optional<BucketAdaptation> createBucketAdaptation(Optional<BucketConversion> bucketConversion, OptionalInt bucketNumber, List<ColumnMapping> columnMappings)
    {
        return bucketConversion.map(conversion -> {
            List<ColumnMapping> baseColumnMapping = columnMappings.stream().filter(mapping -> mapping.getHiveColumnHandle().isBaseColumn()).collect(toList());
            Map<Integer, ColumnMapping> baseHiveColumnToBlockIndex = uniqueIndex(baseColumnMapping, mapping -> mapping.getHiveColumnHandle().getBaseHiveColumnIndex());

            int[] bucketColumnIndices = conversion.getBucketColumnHandles().stream()
                    .mapToInt(columnHandle -> baseHiveColumnToBlockIndex.get(columnHandle.getBaseHiveColumnIndex()).getIndex())
                    .toArray();
            List<HiveType> bucketColumnHiveTypes = conversion.getBucketColumnHandles().stream()
                    .map(columnHandle -> baseHiveColumnToBlockIndex.get(columnHandle.getBaseHiveColumnIndex()).getHiveColumnHandle().getHiveType())
                    .collect(toImmutableList());
            return new BucketAdaptation(
                    bucketColumnIndices,
                    bucketColumnHiveTypes,
                    conversion.getBucketingVersion(),
                    conversion.getTableBucketCount(),
                    conversion.getPartitionBucketCount(),
                    bucketNumber.getAsInt());
        });
    }

    public static class BucketAdaptation
    {
        private final int[] bucketColumnIndices;
        private final List<HiveType> bucketColumnHiveTypes;
        private final BucketingVersion bucketingVersion;
        private final int tableBucketCount;
        private final int partitionBucketCount;
        private final int bucketToKeep;

        public BucketAdaptation(
                int[] bucketColumnIndices,
                List<HiveType> bucketColumnHiveTypes,
                BucketingVersion bucketingVersion,
                int tableBucketCount,
                int partitionBucketCount,
                int bucketToKeep)
        {
            this.bucketColumnIndices = bucketColumnIndices;
            this.bucketColumnHiveTypes = bucketColumnHiveTypes;
            this.bucketingVersion = bucketingVersion;
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.bucketToKeep = bucketToKeep;
        }

        public int[] getBucketColumnIndices()
        {
            return bucketColumnIndices;
        }

        public List<HiveType> getBucketColumnHiveTypes()
        {
            return bucketColumnHiveTypes;
        }

        public BucketingVersion getBucketingVersion()
        {
            return bucketingVersion;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getPartitionBucketCount()
        {
            return partitionBucketCount;
        }

        public int getBucketToKeep()
        {
            return bucketToKeep;
        }
    }
}
