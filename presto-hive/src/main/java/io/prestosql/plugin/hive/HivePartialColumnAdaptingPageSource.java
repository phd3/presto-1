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

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordPageSource;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class HivePartialColumnAdaptingPageSource
        implements ConnectorPageSource
{
    private final RecordPageSource delegate;
    private final ReaderProjectionsAdapter readerProjectionsAdapter;

    public HivePartialColumnAdaptingPageSource(RecordPageSource delegate, ReaderProjectionsAdapter readerProjectionsAdapter)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.readerProjectionsAdapter = requireNonNull(readerProjectionsAdapter, "readerProjectionsAdapter is null");
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
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return readerProjectionsAdapter.adaptPage(delegate.getNextPage());
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }

    RecordPageSource getRecordPageSource()
    {
        return delegate;
    }
}
