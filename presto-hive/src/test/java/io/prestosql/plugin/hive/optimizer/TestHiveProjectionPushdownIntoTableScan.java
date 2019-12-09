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
package io.prestosql.plugin.hive.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.TestingHiveConnectorFactory;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.CatalogContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.any;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScanWithProvidedTableAndColumns;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertTrue;

public class TestHiveProjectionPushdownIntoTableScan
        extends BasePlanTest
{
    private static final String HIVE_CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "test_schema";

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(HIVE_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .build();

    private File baseDir;

    @Override
    protected List<CatalogContext> setupCatalogs()
    {
        baseDir = Files.createTempDir();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        HiveMetastore metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();

        metastore.createDatabase(new HiveIdentity(HIVE_SESSION.toConnectorSession()), database);

        return ImmutableList.of(new CatalogContext(HIVE_CATALOG_NAME, new TestingHiveConnectorFactory(metastore), ImmutableMap.of()));
    }

    @Test
    public void testProjectionPushdown()
    {
        String testTable = "test_simple_projection_pushdown";
        QualifiedObjectName completeTableName = new QualifiedObjectName(HIVE_CATALOG_NAME, SCHEMA_NAME, testTable);

        String tableName = HIVE_CATALOG_NAME + "." + SCHEMA_NAME + "." + testTable;
        getQueryRunner().execute("CREATE TABLE " +
                 tableName + " " +
                "(col0)" +
                " AS " +
                " SELECT cast(row(5, 6) as row(a bigint, b bigint)) as col0 where false");

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = metadataGetter().getTableHandle(session, completeTableName);
        assertTrue(tableHandle.isPresent(), "expected table handle to be present");

        Map<String, ColumnHandle> columns = metadataGetter().getColumnHandles(session, completeTableName);
        assertTrue(columns.containsKey("col0"), "expected column not found");

        HiveColumnHandle baseColumnHandle = (HiveColumnHandle) columns.get("col0");

        HiveColumnHandle projectedColumnForA = createProjectedColumnHandle(baseColumnHandle, ImmutableList.of(0));
        HiveColumnHandle projectedColumnForB = createProjectedColumnHandle(baseColumnHandle, ImmutableList.of(1));

        assertPlan(
                "SELECT col0.a expr_a, col0.b expr_b FROM " + tableName,
                any(tableScanWithProvidedTableAndColumns(
                    tableHandle.get().getConnectorHandle(),
                    TupleDomain.all(),
                    ImmutableMap.of("col0#a", projectedColumnForA, "col0#b", projectedColumnForB))));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
