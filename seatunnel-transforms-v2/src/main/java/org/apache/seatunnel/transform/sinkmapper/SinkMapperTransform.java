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

package org.apache.seatunnel.transform.sinkmapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SinkMapperTransform extends AbstractCatalogSupportMapTransform {

    private final SinkMapperConfig config;
    private final Map<String, SinkMapperConfig.ColumnConfig> columnConfigMap;

    public SinkMapperTransform(SinkMapperConfig config, CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.columnConfigMap = config.getColumnConfigMap();

        // Validate column names exist in table
        if (columnConfigMap != null) {
            for (String colName : columnConfigMap.keySet()) {
                if (inputCatalogTable.getTableSchema().indexOf(colName) < 0) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Column %s not found in table %s, available columns: %s",
                                    colName,
                                    inputCatalogTable.getTablePath(),
                                    inputCatalogTable.getTableSchema().getColumns()));
                }
            }
        }

        // Validate primary key columns exist in table
        if (config.getPrimaryKey() != null && !config.getPrimaryKey().isEmpty()) {
            for (String colName : config.getPrimaryKey()) {
                if (inputCatalogTable.getTableSchema().indexOf(colName) < 0) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Primary key column %s not found in table %s, available columns: %s",
                                    colName,
                                    inputCatalogTable.getTablePath(),
                                    inputCatalogTable.getTableSchema().getColumns()));
                }
            }
        }
    }

    @Override
    public String getPluginName() {
        return SinkMapperConfig.PLUGIN_NAME;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        TableSchema.Builder schemaBuilder =
                TableSchema.builder()
                        .constraintKey(inputCatalogTable.getTableSchema().getConstraintKeys())
                        .columns(transformColumns());

        PrimaryKey originalPrimaryKey = inputCatalogTable.getTableSchema().getPrimaryKey();
        if (config.getPrimaryKey() != null && !config.getPrimaryKey().isEmpty()) {
            schemaBuilder.primaryKey(
                    PrimaryKey.of(
                            inputCatalogTable.getTableId().toTablePath().toString(),
                            config.getPrimaryKey()));
        } else if (originalPrimaryKey != null) {
            schemaBuilder.primaryKey(originalPrimaryKey);
        }

        return schemaBuilder.build();
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        if (outputCatalogTable == null) {
            synchronized (this) {
                if (outputCatalogTable == null) {
                    CatalogTable original = super.getProducedCatalogTable();
                    String comment =
                            config.getTableComment() != null
                                    ? config.getTableComment()
                                    : original.getComment();
                    outputCatalogTable =
                            CatalogTable.of(
                                    original.getTableId(),
                                    original.getTableSchema(),
                                    original.getOptions(),
                                    original.getPartitionKeys(),
                                    comment,
                                    original.getTableId().getCatalogName(),
                                    original.getMetadataSchema());
                }
            }
        }
        return outputCatalogTable;
    }

    private List<Column> transformColumns() {
        return inputCatalogTable.getTableSchema().getColumns().stream()
                .map(
                        column -> {
                            if (columnConfigMap == null
                                    || !columnConfigMap.containsKey(column.getName())) {
                                return column;
                            }
                            SinkMapperConfig.ColumnConfig columnConfig =
                                    columnConfigMap.get(column.getName());

                            // Only PhysicalColumn can have nullable/comment modified
                            if (!(column instanceof PhysicalColumn)) {
                                return column;
                            }

                            PhysicalColumn pc = (PhysicalColumn) column;
                            boolean newNullable =
                                    columnConfig.getNullable() != null
                                            ? columnConfig.getNullable()
                                            : pc.isNullable();
                            String newComment =
                                    columnConfig.getComment() != null
                                            ? columnConfig.getComment()
                                            : pc.getComment();

                            return new PhysicalColumn(
                                    pc.getName(),
                                    pc.getDataType(),
                                    pc.getColumnLength(),
                                    pc.getScale(),
                                    newNullable,
                                    pc.getDefaultValue(),
                                    newComment,
                                    pc.getSourceType(),
                                    pc.getSinkType(),
                                    pc.getOptions() != null ? new HashMap<>(pc.getOptions()) : null,
                                    pc.isUnsigned(),
                                    pc.isZeroFill(),
                                    pc.getBitLen(),
                                    pc.getLongColumnLength());
                        })
                .collect(Collectors.toList());
    }
}
