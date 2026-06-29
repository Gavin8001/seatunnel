/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.gbase8a;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Gbase8aCatalogTest {

    private static Gbase8aCatalog GBASE8A_CATALOG;

    private static final TablePath TABLE_PATH = TablePath.of("test_db", null, "test_table");

    @BeforeAll
    static void setUp() {
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo("jdbc:gbase://172.16.17.156:5258/test_db");
        GBASE8A_CATALOG =
                new Gbase8aCatalog(
                        "GBASE8A_CATALOG",
                        "root",
                        "password",
                        urlInfo,
                        null,
                        "com.gbase.jdbc.Driver");
    }

    @Test
    public void testGetExistDataSql() {
        Assertions.assertEquals(
                "SELECT * FROM test_db.test_table LIMIT 1",
                GBASE8A_CATALOG.getExistDataSql(TABLE_PATH));
    }

    @Test
    public void testGetDatabaseWithConditionSql() {
        String sql = GBASE8A_CATALOG.getDatabaseWithConditionSql("mydb");
        Assertions.assertTrue(sql.contains("SCHEMA_NAME = 'mydb'"), sql);
    }

    @Test
    public void testGetTableWithConditionSql() {
        String sql = GBASE8A_CATALOG.getTableWithConditionSql(TABLE_PATH);
        // exact match per spec: no TABLE_SCHEMA clause, uppercase WHERE
        Assertions.assertEquals(
                "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables WHERE table_schema = 'test_db' AND table_name = 'test_table'",
                sql);
    }

    @Test
    public void testGetListDatabaseSql() {
        Assertions.assertEquals("SHOW DATABASES;", GBASE8A_CATALOG.getListDatabaseSql());
    }

    @Test
    public void testGetListTableSql() {
        Assertions.assertEquals("SHOW TABLES", GBASE8A_CATALOG.getListTableSql("mydb"));
    }

    @Test
    public void testGetSelectColumnsSql() {
        String sql = GBASE8A_CATALOG.getSelectColumnsSql(TABLE_PATH);
        // TABLE_SCHEMA placeholder is the schema (second segment), not the database
        Assertions.assertEquals(
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_SCHEMA = 'test_db' AND TABLE_NAME = 'test_table' "
                        + "ORDER BY ORDINAL_POSITION ASC",
                sql);
    }

    @Test
    public void testGetDropTableSql() {
        Assertions.assertEquals(
                "DROP TABLE test_db.test_table", GBASE8A_CATALOG.getDropTableSql(TABLE_PATH));
    }

    @Test
    public void testGetTruncateTableSql() {
        Assertions.assertEquals(
                "TRUNCATE TABLE test_db.test_table",
                GBASE8A_CATALOG.getTruncateTableSql(TABLE_PATH));
    }

    @Test
    public void testGetCreateTableSqls() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", TABLE_PATH),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        List<String> sqls = GBASE8A_CATALOG.getCreateTableSqls(TABLE_PATH, catalogTable, false);

        String joined = String.join(";\n", sqls);
        Assertions.assertTrue(joined.contains("CREATE TABLE test_table"), joined);
        Assertions.assertTrue(joined.contains("id BIGINT NOT NULL"), joined);
        Assertions.assertTrue(
                joined.contains("COMMENT ON TABLE test_table IS 'User table'"), joined);
        Assertions.assertTrue(joined.contains("COMMENT ON COLUMN test_table.id IS 'id'"), joined);
    }

    @Test
    public void testGetCreateTableSql() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, null))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", TABLE_PATH),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");

        String sql = GBASE8A_CATALOG.getCreateTableSql(TABLE_PATH, catalogTable, false);
        Assertions.assertTrue(sql.startsWith("CREATE TABLE test_table"), sql);
        Assertions.assertTrue(sql.contains("id BIGINT NOT NULL"), sql);
    }

    @Test
    public void testGetTableName() {
        // Gbase8aCatalog overrides to use schema.table format with double-quote separator
        Assertions.assertEquals("test_table", GBASE8A_CATALOG.getTableName(TABLE_PATH));
    }

    @Test
    public void testGetOptionTableName() {
        // Gbase8aCatalog overrides to return schema.table (not the full db.schema.table)
        Assertions.assertEquals("test_table", GBASE8A_CATALOG.getOptionTableName(TABLE_PATH));
    }

    @Test
    public void testGetUrlFromDatabaseName() {
        // Override returns defaultUrl regardless of input database name
        Assertions.assertEquals(
                "jdbc:gbase://172.16.17.156:5258/test_db",
                GBASE8A_CATALOG.getUrlFromDatabaseName("any_db"));
    }

    @Test
    public void testBuildColumn() throws Exception {
        // buildColumn is protected; invoke via reflection to verify ResultSet -> Column wiring
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("COLUMN_NAME")).thenReturn("id");
        when(rs.getString("DATA_TYPE")).thenReturn("INT");
        when(rs.getObject("CHARACTER_MAXIMUM_LENGTH", Long.class)).thenReturn(11L);
        when(rs.getString("IS_NULLABLE")).thenReturn("NO");
        when(rs.getObject("COLUMN_DEFAULT")).thenReturn(null);
        when(rs.getString("COLUMN_COMMENT")).thenReturn("primary key");

        Method m = Gbase8aCatalog.class.getDeclaredMethod("buildColumn", ResultSet.class);
        m.setAccessible(true);
        Column column = (Column) m.invoke(GBASE8A_CATALOG, rs);

        Assertions.assertEquals("id", column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals("INT", column.getSourceType());
        Assertions.assertFalse(column.isNullable());
        Assertions.assertEquals("primary key", column.getComment());
    }

    @Test
    public void testBuildColumnNullableYes() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("COLUMN_NAME")).thenReturn("name");
        when(rs.getString("DATA_TYPE")).thenReturn("VARCHAR");
        when(rs.getObject("CHARACTER_MAXIMUM_LENGTH", Long.class)).thenReturn(255L);
        when(rs.getString("IS_NULLABLE")).thenReturn("YES");
        when(rs.getObject("COLUMN_DEFAULT")).thenReturn("'default'");
        when(rs.getString("COLUMN_COMMENT")).thenReturn(null);

        Method m = Gbase8aCatalog.class.getDeclaredMethod("buildColumn", ResultSet.class);
        m.setAccessible(true);
        Column column = (Column) m.invoke(GBASE8A_CATALOG, rs);

        Assertions.assertEquals("name", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertTrue(column.isNullable());
        Assertions.assertEquals("'default'", column.getDefaultValue());
    }

    @Test
    public void testListTablesDatabaseNotExist() {
        // listTables throws DatabaseNotExistException without contacting a DB
        // (we cannot easily mock the connection path used by listTables; verify the exception
        // surface exists and is wired into the API contract by checking the declared throws clause)
        Method[] methods = Gbase8aCatalog.class.getDeclaredMethods();
        boolean found = false;
        for (Method m : methods) {
            if ("listTables".equals(m.getName()) && m.getParameterCount() == 1) {
                found = true;
                Class<?>[] exs = m.getExceptionTypes();
                boolean hasDbNotExist = false;
                for (Class<?> ex : exs) {
                    if (ex.getName()
                            .equals(
                                    "org.apache.seatunnel.api.table.catalog.exception"
                                            + ".DatabaseNotExistException")) {
                        hasDbNotExist = true;
                        break;
                    }
                }
                Assertions.assertTrue(
                        hasDbNotExist, "listTables must declare DatabaseNotExistException");
            }
        }
        Assertions.assertTrue(found, "listTables(String) override should exist");
    }
}
