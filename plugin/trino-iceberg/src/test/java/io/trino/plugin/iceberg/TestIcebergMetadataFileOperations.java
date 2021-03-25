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
package io.trino.plugin.iceberg;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.TrackingFileIoProvider.OperationContext;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.ResultWithQueryId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.INPUT_FILE_EXISTS;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestIcebergMetadataFileOperations
        extends AbstractTestQueryFramework
{
    private File baseDir;
    private static final MBeanServer BEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String BEAN_NAME = "trino.plugin.iceberg:type=TrackingFileIoProvider,name=iceberg";
    private static final String OPERATION_COUNTS_ATTRIBUTE = "OperationCounts";
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("iceberg")
            .setSchema("test_schema")
            .build();

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                // Tests that inspect MBean attributes need to run with just one node, otherwise
                // the attributes may come from the bound class instance in non-coordinator node
                .setNodeCount(1)
                .build();

        baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("test_version"),
                hdfsEnvironment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));

        queryRunner.installPlugin(new TestingIcebergPlugin(metastore, true));
        queryRunner.createCatalog("iceberg", "iceberg");

        queryRunner.execute("CREATE SCHEMA test_schema");
        return queryRunner;
    }

    @Test
    public void testOperations()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_select_from AS SELECT 1 col0", 1);
        String queryId = runAndGetId("SELECT * FROM test_select_from");

        OperationCounts counts = getCounts().forQueryId(queryId);

        // The table contains exactly one metadata, snapshot and manifest files, so file-naming based filters are sufficient
        // for assumptions.

        String metadataFileSuffix = "metadata.json";
        assertEquals(15, counts.forPathContaining(metadataFileSuffix).forOperation(INPUT_FILE_NEW_STREAM).sum());
        // getLength is cached, so only assert number of different InputFile instances for a file
        assertEquals(0, counts.forPathContaining(metadataFileSuffix).forOperation(INPUT_FILE_GET_LENGTH).get().size());
        assertEquals(0, counts.forPathContaining(metadataFileSuffix).forOperation(INPUT_FILE_EXISTS).sum());

        String snapshotFilePrefix = "/snap-";
        assertEquals(9, counts.forPathContaining(snapshotFilePrefix).forOperation(INPUT_FILE_NEW_STREAM).sum());
        // getLength is cached, so only assert number of different InputFile instances for a file
        assertEquals(9, counts.forPathContaining(snapshotFilePrefix).forOperation(INPUT_FILE_GET_LENGTH).get().size());
        assertEquals(0, counts.forPathContaining(snapshotFilePrefix).forOperation(INPUT_FILE_EXISTS).sum());

        String manifestFile = "-m0.avro";
        assertEquals(2, counts.forPathContaining(manifestFile).forOperation(INPUT_FILE_NEW_STREAM).sum());
        // getLength is cached, so only assert number of different InputFile instances for a file
        assertEquals(1, counts.forPathContaining(manifestFile).forOperation(INPUT_FILE_GET_LENGTH).get().size());
        assertEquals(0, counts.forPathContaining(manifestFile).forOperation(INPUT_FILE_EXISTS).sum());
    }

    private String runAndGetId(String query)
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(TEST_SESSION, query);
        return result.getQueryId().getId();
    }

    private static OperationCounts getCounts()
            throws Exception
    {
        Map<OperationContext, Integer> counts = (Map<OperationContext, Integer>) BEAN_SERVER.getAttribute(new ObjectName(BEAN_NAME), OPERATION_COUNTS_ATTRIBUTE);
        return new OperationCounts(counts);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }

    private static class OperationCounts
    {
        private final Map<OperationContext, Integer> allCounts;
        private Optional<String> queryId = Optional.empty();
        private Predicate<String> pathPredicate = Predicates.alwaysTrue();
        private Optional<TrackingFileIoProvider.OperationType> type = Optional.empty();

        public OperationCounts(Map<OperationContext, Integer> allCounts)
        {
            this.allCounts = requireNonNull(allCounts, "allCounts is null");
        }

        public OperationCounts forQueryId(String queryId)
        {
            requireNonNull(queryId, "queryId is null");
            this.queryId = Optional.of(queryId);
            return this;
        }

        public OperationCounts forPathContaining(String value)
        {
            this.pathPredicate = path -> path.contains(value);
            return this;
        }

        public OperationCounts forOperation(TrackingFileIoProvider.OperationType type)
        {
            this.type = Optional.of(type);
            return this;
        }

        public Map<OperationContext, Integer> get()
        {
            return allCounts.entrySet().stream()
                    .filter(entry -> queryId.map(id -> entry.getKey().getQueryId().equals(id)).orElse(true))
                    .filter(entry -> pathPredicate.test(entry.getKey().getFilePath()))
                    .filter(entry -> type.map(value -> value == entry.getKey().getOperationType()).orElse(true))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public int sum()
        {
            Map<OperationContext, Integer> filteredMap = get();
            return filteredMap.entrySet().stream()
                    .map(Map.Entry::getValue)
                    .reduce(0, Integer::sum);
        }
    }
}
