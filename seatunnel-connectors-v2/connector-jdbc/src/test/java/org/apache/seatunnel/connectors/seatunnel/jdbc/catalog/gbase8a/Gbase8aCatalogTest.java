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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Gbase8aCatalogTest {

    private static Gbase8aCatalog GBASE8A_CATALOG;

    private static final TablePath TABLE_PATH =
            TablePath.of("test_db", "test_schema", "test_table");

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
                "SELECT * FROM \"test_schema\".\"test_table\" LIMIT 1",
                GBASE8A_CATALOG.getExistDataSql(TABLE_PATH));
    }

    @Test
    public void testGetDatabaseWithConditionSql() {
        String sql = GBASE8A_CATALOG.getDatabaseWithConditionSql("mydb");
        Assertions.assertTrue(sql.contains("name = 'mydb'"), sql);
    }

    @Test
    public void testGetTableWithConditionSql() {
        String sql = GBASE8A_CATALOG.getTableWithConditionSql(TABLE_PATH);
        Assertions.assertTrue(sql.contains("TABLE_NAME = 'test_table'"), sql);
    }

    @Test
    public void testGetListDatabaseSql() {
        Assertions.assertEquals("SHOW DATABASES;", GBASE8A_CATALOG.getListDatabaseSql());
    }

    @Test
    public void testGetListTableSql() {
        Assertions.assertEquals("SHOW TABLES;", GBASE8A_CATALOG.getListTableSql("mydb"));
    }

    @Test
    public void testGetSelectColumnsSql() {
        String sql = GBASE8A_CATALOG.getSelectColumnsSql(TABLE_PATH);
        Assertions.assertTrue(sql.contains("INFORMATION_SCHEMA.COLUMNS"), sql);
        Assertions.assertTrue(sql.contains("TABLE_SCHEMA = 'test_schema'"), sql);
        Assertions.assertTrue(sql.contains("TABLE_NAME = 'test_table'"), sql);
    }

    @Test
    public void testGetDropTableSql() {
        Assertions.assertEquals(
                "DROP TABLE \"test_schema\".\"test_table\"",
                GBASE8A_CATALOG.getDropTableSql(TABLE_PATH));
    }

    @Test
    public void testGetTruncateTableSql() {
        Assertions.assertEquals(
                "TRUNCATE TABLE \"test_schema\".\"test_table\"",
                GBASE8A_CATALOG.getTruncateTableSql(TABLE_PATH));
    }

    @Test
    public void testCreateDatabaseInternalUnsupported() {
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> GBASE8A_CATALOG.createDatabaseInternal("mydb"));
    }

    @Test
    public void testDropDatabaseInternalUnsupported() {
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> GBASE8A_CATALOG.dropDatabaseInternal("mydb"));
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
        Assertions.assertTrue(
                joined.contains("CREATE TABLE \"test_schema\".\"test_table\""), joined);
        Assertions.assertTrue(joined.contains("\"id\" BIGINT NOT NULL"), joined);
        Assertions.assertTrue(
                joined.contains("COMMENT ON TABLE \"test_schema\".\"test_table\" IS 'User table'"),
                joined);
        Assertions.assertTrue(
                joined.contains("COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id'"),
                joined);
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
        Assertions.assertTrue(sql.startsWith("CREATE TABLE \"test_schema\".\"test_table\""), sql);
        Assertions.assertTrue(sql.contains("\"id\" BIGINT NOT NULL"), sql);
    }
}
