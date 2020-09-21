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
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.sql.planner.assertions.BasePushdownPlanTest;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIcebergTable;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.any;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestIcebergProjectionPushdown
        extends BasePushdownPlanTest
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";
    private File baseDir;
    private HiveMetastore metastore;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        baseDir = Files.createTempDir();

        // Create metastore
        metastore = new FileHiveMetastore(getDefaultHdfsEnvironment(), baseDir.toURI().toString(), "test", true);

        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(metastore)),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
        metastore.createDatabase(new HiveIdentity(session.toConnectorSession()), database);

        return queryRunner;
    }

    private static HdfsEnvironment getDefaultHdfsEnvironment()
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        return new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
    }

    @Test
    public void testDereferencePushdown()
    {
        String testTable = "test_simple_projection_pushdown";
        QualifiedObjectName completeTableName = new QualifiedObjectName(ICEBERG_CATALOG, SCHEMA_NAME, testTable);

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0, col1) AS" +
                        " SELECT cast(row(5, 6) as row(x bigint, y bigint)) AS col0, 5 AS col1 WHERE false",
                testTable));

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertTrue(tableHandle.isPresent(), "expected the table handle to be present");

        org.apache.iceberg.Table icebergTable = getIcebergTable(
                metastore,
                getDefaultHdfsEnvironment(),
                session.toConnectorSession(),
                completeTableName.asSchemaTableName());

        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        IcebergColumnHandle column0Handle = (IcebergColumnHandle) columns.get("col0");
        IcebergColumnHandle column1Handle = (IcebergColumnHandle) columns.get("col1");

        IcebergColumnHandle columnX = new IcebergColumnHandle(column0Handle.getBaseId(),
                column0Handle.getBaseName(),
                column0Handle.getBaseType(),
                column0Handle.getComment(),
                Optional.of(new IcebergColumnHandle.Projection(
                        BIGINT,
                        icebergTable.schema().findField("col0.x").fieldId(),
                        ImmutableList.of(0),
                        ImmutableList.of("x"))));

        IcebergColumnHandle columnY = new IcebergColumnHandle(column0Handle.getBaseId(),
                column0Handle.getBaseName(),
                column0Handle.getBaseType(),
                column0Handle.getComment(),
                Optional.of(new IcebergColumnHandle.Projection(
                        BIGINT,
                        icebergTable.schema().findField("col0.y").fieldId(),
                        ImmutableList.of(1),
                        ImmutableList.of("y"))));

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + testTable,
                any(tableScan(
                        equalTo(tableHandle.get().getConnectorHandle()),
                        TupleDomain.all(),
                        ImmutableMap.of("col0#x", equalTo(columnX), "col0#y", equalTo(columnY)))));

        // TODO: enable the following after #4932 goes in
//        // Projection and predicate pushdown
//        assertPlan(
//                format("SELECT col0.x FROM %s WHERE col0.x = col1 + 3 and col0.y = 2", testTable),
//                anyTree(
//                        filter(
//                                "col0_y = BIGINT '2' AND (col0_x =  cast((col1 + 3) as BIGINT))",
//                                tableScan(
//                                        table -> ((IcebergTableHandle) table).getPredicate().getDomains().get()
//                                                .equals(ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L))),
//                                        TupleDomain.all(),
//                                        ImmutableMap.of("col0_y", equalTo(columnY), "col0_x", equalTo(columnX), "col1", equalTo(column1Handle))))));
//
//        // Projection and predicate pushdown with overlapping columns
//        assertPlan(
//                format("SELECT col0, col0.y expr_y FROM %s WHERE col0.x = 5", testTable),
//                anyTree(
//                        filter(
//                                "col0_x = BIGINT '5'",
//                                tableScan(
//                                        table -> ((IcebergTableHandle) table).getPredicate().getDomains().get()
//                                                .equals(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L))),
//                                        TupleDomain.all(),
//                                        ImmutableMap.of("col0", equalTo(column0Handle), "col0_x", equalTo(columnX))))));
//
//        // Projection and predicate pushdown with joins
//        assertPlan(
//                format("SELECT T.col0.x, T.col0, T.col0.y FROM %s T join %s S on T.col1 = S.col1 WHERE (T.col0.x = 2)", testTable, testTable),
//                anyTree(
//                        project(
//                                ImmutableMap.of(
//                                        "expr_0_x", expression("expr_0.x"),
//                                        "expr_0", expression("expr_0"),
//                                        "expr_0_y", expression("expr_0.y")),
//                                join(
//                                        INNER,
//                                        ImmutableList.of(equiJoinClause("t_expr_1", "s_expr_1")),
//                                        anyTree(
//                                                filter(
//                                                        "expr_0_x = BIGINT '2'",
//                                                        tableScan(
//                                                                table -> ((IcebergTableHandle) table).getPredicate().getDomains().get()
//                                                                        .equals(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L))),
//                                                                TupleDomain.all(),
//                                                                ImmutableMap.of("expr_0_x", equalTo(columnX), "expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle))))),
//                                        anyTree(
//                                                tableScan(
//                                                        equalTo(tableHandle.get().getConnectorHandle()),
//                                                        TupleDomain.all(),
//                                                        ImmutableMap.of("s_expr_1", equalTo(column1Handle))))))));
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
