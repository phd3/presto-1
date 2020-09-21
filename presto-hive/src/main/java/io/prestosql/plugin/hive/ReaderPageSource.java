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

import io.prestosql.spi.connector.ConnectorPageSource;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper class for
 * - delegate reader page source and
 * - columns to be returned by the delegate
 * <p>
 * Empty {@param columns} indicates that the delegate page source reads the exact same columns provided to
 * it in {@link HivePageSourceFactory#createPageSource}
 */
public class ReaderPageSource
{
    private final ConnectorPageSource pageSource;
    private final Optional<ReaderColumns> columns;

    public ReaderPageSource(ConnectorPageSource pageSource, Optional<ReaderColumns> columns)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    public ConnectorPageSource get()
    {
        return pageSource;
    }

    public Optional<ReaderColumns> getColumns()
    {
        return columns;
    }
}
