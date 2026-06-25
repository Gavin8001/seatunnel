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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import java.util.List;

public class Gbase8aCatalog extends AbstractJdbcCatalog {

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
        throw new UnsupportedOperationException();
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
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
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SHOW DATABASES;";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SHOW TABLES;";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                tablePath.getSchemaName(), tablePath.getTableName());
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
}
