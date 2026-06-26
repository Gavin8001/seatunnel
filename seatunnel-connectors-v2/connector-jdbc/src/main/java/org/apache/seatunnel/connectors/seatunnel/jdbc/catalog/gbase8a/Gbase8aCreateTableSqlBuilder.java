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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gbase8a.Gbase8aTypeConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Gbase8aCreateTableSqlBuilder extends AbstractJdbcCreateTableSqlBuilder {

    private final List<Column> columns;
    private final PrimaryKey primaryKey;
    private final String sourceCatalogName;
    private final String fieldIde;
    private final String tableComment;
    private final List<ConstraintKey> constraintKeys;
    private final boolean createIndex;

    public Gbase8aCreateTableSqlBuilder(CatalogTable catalogTable, boolean createIndex) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.tableComment = catalogTable.getComment();
        this.constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
        this.createIndex = createIndex;
    }

    public List<String> build(TablePath tablePath) {
        List<String> sqls = new ArrayList<>();

        StringBuilder createTableSql = new StringBuilder();
        String tableName = tablePath.getSchemaAndTableName("");

        //creat table
        createTableSql.append("CREATE TABLE ").append(tableName).append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(column -> CatalogUtils.getFieldIde(buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        if (createIndex
                && primaryKey != null
                && CollectionUtils.isNotEmpty(primaryKey.getColumnNames())) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");

        sqls.add(createTableSql.toString());

        //table index
        if (createIndex && CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())
                        || (primaryKey != null
                                && (StringUtils.equals(
                                                primaryKey.getPrimaryKey(),
                                                constraintKey.getConstraintName())
                                        || primaryContainsAllConstrainKey(
                                                primaryKey, constraintKey)))) {
                    continue;
                }
                String constraintKeySql = buildConstraintKeySql(tableName, constraintKey);
                if (StringUtils.isNotEmpty(constraintKeySql)) {
                    sqls.add(constraintKeySql);
                }
            }
        }

        // Table comment
        if (StringUtils.isNotBlank(tableComment)) {
            sqls.add("COMMENT ON TABLE " + tableName + " IS '" + tableComment + "'");
        }

        // Column comments
        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(column -> buildColumnCommentSql(column, tableName))
                        .collect(Collectors.toList());
        sqls.addAll(commentSqls);

        return sqls;
    }

    String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append(column.getName()).append(" ");

        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (StringUtils.equals(DatabaseIdentifier.GBASE_8A, sourceCatalogName)
                && StringUtils.isNotEmpty(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = Gbase8aTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        String columnNamesString =
                primaryKey.getColumnNames().stream().collect(Collectors.joining(", "));
        return "CONSTRAINT "
                + primaryKey.getPrimaryKey()
                + "_"
                + randomSuffix
                + " PRIMARY KEY ("
                + columnNamesString
                + ")";
    }

    private String buildColumnCommentSql(Column column, String tableName) {
        return "COMMENT ON COLUMN "
                + tableName
                + "."
                + column.getName()
                + " IS '"
                + column.getComment()
                + "'";
    }

    private String buildConstraintKeySql(String tableName, ConstraintKey constraintKey) {
        ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        String constraintName = constraintKey.getConstraintName();
        if (constraintName.length() > 25) {
            constraintName = constraintName.substring(0, 25);
        }
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(c -> c.getColumnName())
                        .collect(Collectors.joining(", "));

        if (constraintType == ConstraintKey.ConstraintType.INDEX_KEY) {
            return String.format(
                    "CREATE INDEX %s ON %s (%s) USING HASH GLOBAL",
                    constraintName + "_" + randomSuffix, tableName, indexColumns);
        }
        return null;
    }
}
