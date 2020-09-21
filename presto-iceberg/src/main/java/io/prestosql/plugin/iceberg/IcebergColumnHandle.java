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
package io.prestosql.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IcebergColumnHandle
        implements ColumnHandle
{
    private final int baseId;
    private final String baseName;
    private final Type baseType;
    private final Optional<String> comment;
    private final Optional<Projection> projection;

    private final int id;
    private final String name;
    private final Type type;

    @JsonCreator
    public IcebergColumnHandle(
            @JsonProperty("baseId") int baseId,
            @JsonProperty("baseName") String baseName,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("projection") Optional<Projection> projection)
    {
        this.baseId = baseId;
        this.baseName = requireNonNull(baseName, "baseName is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.projection = requireNonNull(projection, "projection is null");

        this.name = projection.isPresent() ? baseName + "." + projection.get().getPartialName() : baseName;
        this.id = projection.isPresent() ? projection.get().getId() : baseId;
        this.type = projection.isPresent() ? projection.get().getType() : baseType;
    }

    @JsonProperty
    public int getBaseId()
    {
        return baseId;
    }

    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getBaseName()
    {
        return baseName;
    }

    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Optional<Projection> getProjection()
    {
        return projection;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseId, baseName, baseType, comment, projection);
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
        IcebergColumnHandle other = (IcebergColumnHandle) obj;
        return this.baseId == other.baseId &&
                Objects.equals(this.baseName, other.baseName) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.projection, other.projection);
    }

    @Override
    public String toString()
    {
        return getId() + ":" + getName() + ":" + getType().getDisplayName();
    }

    public boolean identityProjection()
    {
        return !projection.isPresent();
    }

    public IcebergColumnHandle getBaseColumn()
    {
        return new IcebergColumnHandle(baseId, baseName, baseType, comment, Optional.empty());
    }

    public static class Projection
    {
        private final Type type;
        private final int id;
        private final String partialName;
        private final List<Integer> fieldPositions;
        private final List<String> fieldNames;

        @JsonCreator
        public Projection(
                @JsonProperty("type") Type type,
                @JsonProperty("id") int id,
                @JsonProperty("fieldPositions") List<Integer> fieldPositions,
                @JsonProperty("fieldNames") List<String> fieldNames)
        {
            this.type = requireNonNull(type, "type is null");
            this.id = id;

            this.fieldPositions = requireNonNull(fieldPositions, "fieldIndices is null");
            this.fieldNames = requireNonNull(fieldNames, "fieldNames is null");

            checkArgument(fieldNames.size() == fieldPositions.size(), "fieldPositions and fieldNames should have the same size");

            this.partialName = String.join(".", fieldNames);
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public int getId()
        {
            return id;
        }

        @JsonProperty
        public List<Integer> getFieldPositions()
        {
            return fieldPositions;
        }

        @JsonProperty
        public List<String> getFieldNames()
        {
            return fieldNames;
        }

        public String getPartialName()
        {
            return partialName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, id, fieldNames, fieldPositions);
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
            Projection other = (Projection) obj;
            return Objects.equals(this.type, other.type) &&
                    Objects.equals(this.id, other.id) &&
                    Objects.equals(this.fieldNames, other.fieldNames) &&
                    Objects.equals(this.fieldPositions, other.fieldPositions);
        }
    }
}
