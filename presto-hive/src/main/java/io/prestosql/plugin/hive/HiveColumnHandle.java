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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class HiveColumnHandle
        implements ColumnHandle
{
    public static final int PATH_COLUMN_INDEX = -11;
    public static final String PATH_COLUMN_NAME = "$path";
    public static final HiveType PATH_HIVE_TYPE = HIVE_STRING;
    public static final Type PATH_TYPE = VARCHAR;

    public static final int BUCKET_COLUMN_INDEX = -12;
    public static final String BUCKET_COLUMN_NAME = "$bucket";
    public static final HiveType BUCKET_HIVE_TYPE = HIVE_INT;
    public static final Type BUCKET_TYPE_SIGNATURE = INTEGER;

    public static final int FILE_SIZE_COLUMN_INDEX = -13;
    public static final String FILE_SIZE_COLUMN_NAME = "$file_size";
    public static final HiveType FILE_SIZE_TYPE = HIVE_LONG;
    public static final Type FILE_SIZE_TYPE_SIGNATURE = BIGINT;

    public static final int FILE_MODIFIED_TIME_COLUMN_INDEX = -14;
    public static final String FILE_MODIFIED_TIME_COLUMN_NAME = "$file_modified_time";
    public static final HiveType FILE_MODIFIED_TIME_TYPE = HIVE_LONG;
    public static final Type FILE_MODIFIED_TIME_TYPE_SIGNATURE = BIGINT;

    private static final String UPDATE_ROW_ID_COLUMN_NAME = "$shard_row_id";

    public enum ColumnType
    {
        PARTITION_KEY,
        REGULAR,
        SYNTHESIZED,
    }

    private final String baseColumnName;
    private final int baseHiveColumnIndex;
    private final HiveType baseColumnHiveType;
    private final Type baseColumnType;

    private final List<String> dereferenceFieldNames;
    private final List<Integer> dereferenceFieldIndices;

    private final String name;

    private final HiveType hiveType;
    private final Type type;

    private final ColumnType columnType;
    private final Optional<String> comment;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("baseColumnName") String baseColumnName,
            @JsonProperty("baseHiveColumnIndex") int baseHiveColumnIndex,
            @JsonProperty("baseColumnHiveType") HiveType baseColumnHiveType,
            @JsonProperty("baseColumnType") Type baseColumnType,
            @JsonProperty("dereferenceFieldNames") List<String> dereferenceFieldNames,
            @JsonProperty("dereferenceFieldIndices") List<Integer> dereferenceFieldIndices,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("type") Type type,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.baseColumnName = requireNonNull(baseColumnName, "baseColumnName is null");
        checkArgument(baseHiveColumnIndex >= 0 || columnType == PARTITION_KEY || columnType == SYNTHESIZED, "baseHiveColumnIndex is negative");
        this.baseHiveColumnIndex = baseHiveColumnIndex;
        this.baseColumnHiveType = baseColumnHiveType;
        this.baseColumnType = baseColumnType;

        this.dereferenceFieldNames = ImmutableList.copyOf(requireNonNull(dereferenceFieldNames, "dereferenceFieldNames is null"));
        this.dereferenceFieldIndices = ImmutableList.copyOf(requireNonNull(dereferenceFieldIndices, "dereferenceFieldIndices is null"));
        checkArgument(dereferenceFieldIndices.size() == dereferenceFieldNames.size(), "dereference field names and indices should have the same sizes");

        this.name = this.baseColumnName + dereferenceFieldNames.stream().map(name -> "#" + name).collect(Collectors.joining());

        this.hiveType = requireNonNull(hiveType, "hiveType is null");
        this.type = requireNonNull(type, "type is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public static HiveColumnHandle createTopLevelHiveColumnHandle(
            String topLevelColumnName,
            int topLevelColumnIndex,
            HiveType hiveType,
            Type type,
            ColumnType columnType,
            Optional<String> comment)
    {
        return new HiveColumnHandle(topLevelColumnName, topLevelColumnIndex, hiveType, type, ImmutableList.of(), ImmutableList.of(), hiveType, type, columnType, comment);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getBaseColumnName()
    {
        return baseColumnName;
    }

    @JsonProperty
    public HiveType getBaseColumnHiveType()
    {
        return baseColumnHiveType;
    }

    @JsonProperty
    public Type getBaseColumnType()
    {
        return baseColumnType;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public int getBaseHiveColumnIndex()
    {
        return baseHiveColumnIndex;
    }

    public boolean isPartitionKey()
    {
        return columnType == PARTITION_KEY;
    }

    public boolean isHidden()
    {
        return columnType == SYNTHESIZED;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, null, isHidden());
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public List<String> getDereferenceFieldNames()
    {
        return dereferenceFieldNames;
    }

    @JsonProperty
    public List<Integer> getDereferenceFieldIndices()
    {
        return dereferenceFieldIndices;
    }

    public boolean isBaseColumn()
    {
        return dereferenceFieldIndices.size() == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseColumnName, baseHiveColumnIndex, dereferenceFieldIndices, dereferenceFieldNames, name, hiveType, columnType, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HiveColumnHandle other = (HiveColumnHandle) obj;
        return Objects.equals(this.baseColumnName, other.baseColumnName) &&
                Objects.equals(this.baseHiveColumnIndex, other.baseHiveColumnIndex) &&
                Objects.equals(this.baseColumnHiveType, other.baseColumnHiveType) &&
                Objects.equals(this.baseColumnType, other.baseColumnType) &&
                Objects.equals(this.dereferenceFieldIndices, other.dereferenceFieldIndices) &&
                Objects.equals(this.dereferenceFieldNames, other.dereferenceFieldNames) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.hiveType, other.hiveType) &&
                this.columnType == other.columnType &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return name + ":" + hiveType + ":" + ":" + columnType;
    }

    public static HiveColumnHandle updateRowIdHandle()
    {
        // Hive connector only supports metadata delete. It does not support generic row-by-row deletion.
        // Metadata delete is implemented in Presto by generating a plan for row-by-row delete first,
        // and then optimize it into metadata delete. As a result, Hive connector must provide partial
        // plan-time support for row-by-row delete so that planning doesn't fail. This is why we need
        // rowid handle. Note that in Hive connector, rowid handle is not implemented beyond plan-time.

        return createTopLevelHiveColumnHandle(UPDATE_ROW_ID_COLUMN_NAME, -1, HIVE_LONG, BIGINT, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle pathColumnHandle()
    {
        return createTopLevelHiveColumnHandle(PATH_COLUMN_NAME, PATH_COLUMN_INDEX, PATH_HIVE_TYPE, PATH_TYPE, SYNTHESIZED, Optional.empty());
    }

    /**
     * The column indicating the bucket id.
     * When table bucketing differs from partition bucketing, this column indicates
     * what bucket the row will fall in under the table bucketing scheme.
     */
    public static HiveColumnHandle bucketColumnHandle()
    {
        return createTopLevelHiveColumnHandle(BUCKET_COLUMN_NAME, BUCKET_COLUMN_INDEX, BUCKET_HIVE_TYPE, BUCKET_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle fileSizeColumnHandle()
    {
        return createTopLevelHiveColumnHandle(FILE_SIZE_COLUMN_NAME, FILE_SIZE_COLUMN_INDEX, FILE_SIZE_TYPE, FILE_SIZE_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle fileModifiedTimeColumnHandle()
    {
        return createTopLevelHiveColumnHandle(FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_INDEX, FILE_MODIFIED_TIME_TYPE, FILE_MODIFIED_TIME_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static boolean isPathColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == PATH_COLUMN_INDEX;
    }

    public static boolean isBucketColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == BUCKET_COLUMN_INDEX;
    }

    public static boolean isFileSizeColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == FILE_SIZE_COLUMN_INDEX;
    }

    public static boolean isFileModifiedTimeColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == FILE_MODIFIED_TIME_COLUMN_INDEX;
    }
}
