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

import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.ReaderProjectionsAdapter.ChannelMapping;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Applies projections on delegate fields provided by {@link ChannelMapping} to produce fields expected from this cursor.
 */
public class HiveReaderProjectionsAdaptingRecordCursor
        implements RecordCursor
{
    private final RecordCursor delegate;
    private final ChannelMapping[] channelMappings;
    private final Type[] outputTypes;
    private final Type[] inputTypes;

    public HiveReaderProjectionsAdaptingRecordCursor(RecordCursor delegate, ReaderProjectionsAdapter projectionsAdapter)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(projectionsAdapter, "projectionsAdapter is null");

        this.channelMappings = new ChannelMapping[projectionsAdapter.getOutputToInputMapping().size()];
        projectionsAdapter.getOutputToInputMapping().toArray(channelMappings);

        this.outputTypes = new Type[projectionsAdapter.getOutputTypes().size()];
        projectionsAdapter.getOutputTypes().toArray(outputTypes);

        this.inputTypes = new Type[projectionsAdapter.getInputTypes().size()];
        projectionsAdapter.getInputTypes().toArray(inputTypes);
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public Type getType(int field)
    {
        return outputTypes[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        return delegate.advanceNextPosition();
    }

    @Override
    public boolean getBoolean(int field)
    {
        int inputFieldIndex = channelMappings[field].getInputChannelIndex();
        List<Integer> dereferences = channelMappings[field].getDereferenceSequence();

        if (dereferences.isEmpty()) {
            return delegate.getBoolean(inputFieldIndex);
        }

        Block baseObject = (Block) delegate.getObject(inputFieldIndex);
        Type baseType = inputTypes[inputFieldIndex];

        for (int j = 0; j < dereferences.size() - 1; j++) {
            int dereferenceIndex = dereferences.get(j);
            baseObject = baseObject.getObject(dereferenceIndex, Block.class);
            baseType = baseType.getTypeParameters().get(dereferenceIndex);
        }

        int finalDereference = dereferences.get(dereferences.size() - 1);
        return baseType.getTypeParameters().get(finalDereference).getBoolean(baseObject, finalDereference);
    }

    @Override
    public long getLong(int field)
    {
        int inputFieldIndex = channelMappings[field].getInputChannelIndex();
        List<Integer> dereferences = channelMappings[field].getDereferenceSequence();

        if (dereferences.isEmpty()) {
            return delegate.getLong(inputFieldIndex);
        }

        Block baseObject = (Block) delegate.getObject(inputFieldIndex);
        Type baseType = inputTypes[inputFieldIndex];

        for (int j = 0; j < dereferences.size() - 1; j++) {
            int dereferenceIndex = dereferences.get(j);
            baseObject = baseObject.getObject(dereferenceIndex, Block.class);
            baseType = baseType.getTypeParameters().get(dereferenceIndex);
        }

        int finalDereference = dereferences.get(dereferences.size() - 1);
        return baseType.getTypeParameters().get(finalDereference).getLong(baseObject, finalDereference);
    }

    @Override
    public double getDouble(int field)
    {
        int inputFieldIndex = channelMappings[field].getInputChannelIndex();
        List<Integer> dereferences = channelMappings[field].getDereferenceSequence();

        if (dereferences.isEmpty()) {
            return delegate.getDouble(inputFieldIndex);
        }

        Block baseObject = (Block) delegate.getObject(inputFieldIndex);
        Type baseType = inputTypes[inputFieldIndex];

        for (int j = 0; j < dereferences.size() - 1; j++) {
            int dereferenceIndex = dereferences.get(j);
            baseObject = baseObject.getObject(dereferenceIndex, Block.class);
            baseType = baseType.getTypeParameters().get(dereferenceIndex);
        }

        int finalDereference = dereferences.get(dereferences.size() - 1);
        return baseType.getTypeParameters().get(finalDereference).getDouble(baseObject, finalDereference);
    }

    @Override
    public Slice getSlice(int field)
    {
        int inputFieldIndex = channelMappings[field].getInputChannelIndex();
        List<Integer> dereferences = channelMappings[field].getDereferenceSequence();

        if (dereferences.isEmpty()) {
            return delegate.getSlice(inputFieldIndex);
        }

        Block baseObject = (Block) delegate.getObject(inputFieldIndex);
        Type baseType = inputTypes[inputFieldIndex];

        for (int j = 0; j < dereferences.size() - 1; j++) {
            int dereferenceIndex = dereferences.get(j);
            baseObject = baseObject.getObject(dereferenceIndex, Block.class);
            baseType = baseType.getTypeParameters().get(dereferenceIndex);
        }

        int finalDereference = dereferences.get(dereferences.size() - 1);
        return baseType.getTypeParameters().get(finalDereference).getSlice(baseObject, finalDereference);
    }

    @Override
    public Object getObject(int field)
    {
        int inputFieldIndex = channelMappings[field].getInputChannelIndex();
        List<Integer> dereferences = channelMappings[field].getDereferenceSequence();

        if (dereferences.isEmpty()) {
            return delegate.getObject(inputFieldIndex);
        }

        Block baseObject = (Block) delegate.getObject(inputFieldIndex);
        Type baseType = inputTypes[inputFieldIndex];

        for (int j = 0; j < dereferences.size() - 1; j++) {
            int dereferenceIndex = dereferences.get(j);
            baseObject = baseObject.getObject(dereferenceIndex, Block.class);
            baseType = baseType.getTypeParameters().get(dereferenceIndex);
        }

        int finalDereference = dereferences.get(dereferences.size() - 1);
        return baseType.getTypeParameters().get(finalDereference).getObject(baseObject, finalDereference);
    }

    @Override
    public boolean isNull(int field)
    {
        int inputFieldIndex = channelMappings[field].getInputChannelIndex();
        List<Integer> dereferences = channelMappings[field].getDereferenceSequence();

        if (dereferences.isEmpty()) {
            return delegate.isNull(inputFieldIndex);
        }

        if (delegate.isNull(inputFieldIndex)) {
            return true;
        }

        Block baseObject = (Block) delegate.getObject(inputFieldIndex);

        for (int j = 0; j < dereferences.size() - 1; j++) {
            int dereferenceIndex = dereferences.get(j);
            if (baseObject.isNull(dereferenceIndex)) {
                return true;
            }
            baseObject = baseObject.getObject(dereferenceIndex, Block.class);
        }

        int finalDereference = dereferences.get(dereferences.size() - 1);
        return baseObject.isNull(finalDereference);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
