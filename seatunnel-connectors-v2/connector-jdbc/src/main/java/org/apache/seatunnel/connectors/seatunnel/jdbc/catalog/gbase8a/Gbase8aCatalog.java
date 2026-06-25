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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gbase8a.Gbase8aTypeConverter;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class Gbase8aCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL =
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
                    + "IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' "
                    + "ORDER BY ORDINAL_POSITION ASC";

    public Gbase8aCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
    }

    @Override
    protected void createDatabaseInternal(String databaseName) {
        throw new UnsupportedOperationException("GBase 8A does not support creating databases");
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) {
        throw new UnsupportedOperationException("GBase 8A does not support dropping databases");
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT * FROM \"%s\".\"%s\" LIMIT 1",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where name = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return getListTableSql(tablePath.getDatabaseName())
                + " WHERE TABLE_NAME = '"
                + tablePath.getTableName()
                + "'";
    }

    @Override
    protected String getListDatabaseSql() {
        return "SHOW DATABASES;";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SHOW TABLES";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("DATA_TYPE");
        Long columnLength = resultSet.getObject("CHARACTER_MAXIMUM_LENGTH", Long.class);
        boolean isNullable = "YES".equalsIgnoreCase(resultSet.getString("IS_NULLABLE"));
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        String columnComment = resultSet.getString("COLUMN_COMMENT");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();

        return Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE \"%s\".\"%s\"", tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return getCreateTableSqls(tablePath, table, createIndex).get(0);
    }

    @Override
    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new Gbase8aCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName("\"");
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        try {
            return queryString(dbUrl, getListTableSql(databaseName), this::getTableName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing table in catalog %s", catalogName), e);
        }
    }
}
