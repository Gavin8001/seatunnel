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
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Gbase8aCreateTableSqlBuilderTest {

    @Test
    public void testBuildCreateTableSql() {
        TablePath tablePath = TablePath.of("test_db", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "create_time",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "create_time"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                Arrays.asList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null)))))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        List<String> sqls = new Gbase8aCreateTableSqlBuilder(catalogTable, true).build(tablePath);
        String createTableSql = String.join(";\n", sqls) + ";";

        Assertions.assertTrue(
                createTableSql.contains("CREATE TABLE \"test_schema\".\"test_table\""));
        Assertions.assertTrue(createTableSql.contains("\"id\" BIGINT NOT NULL"));
        Assertions.assertTrue(createTableSql.contains("\"name\" VARCHAR(128) NOT NULL"));
        Assertions.assertTrue(
                createTableSql.contains(
                        "COMMENT ON TABLE \"test_schema\".\"test_table\" IS 'User table'"));
        Assertions.assertTrue(
                createTableSql.contains(
                        "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id'"));
        Assertions.assertTrue(createTableSql.contains("PRIMARY KEY (\"id\")"));
        Assertions.assertTrue(createTableSql.contains("UNIQUE (\"name\")"));
    }

    @Test
    public void testColumnSinkType() {
        Gbase8aCreateTableSqlBuilder sqlBuilder = mock(Gbase8aCreateTableSqlBuilder.class);

        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR(10)");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        when(sqlBuilder.buildColumnSql(column)).thenCallRealMethod();

        String result = sqlBuilder.buildColumnSql(column);

        Assertions.assertEquals("\"col1\" VARCHAR(10) NOT NULL", result);
    }
}
