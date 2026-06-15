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

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SinkMapperConfig implements Serializable {

    public static final String PLUGIN_NAME = "SinkMapper";

    public static final Option<String> TABLE_COMMENT =
            Options.key("table_comment")
                    .type(new TypeReference<String>() {})
                    .noDefaultValue()
                    .withDescription("The comment of the table");

    public static final Option<List<String>> PRIMARY_KEY =
            Options.key("primary_key")
                    .type(new TypeReference<List<String>>() {})
                    .noDefaultValue()
                    .withDescription("The primary key columns");

    public static final Option<List<ColumnConfig>> COLUMNS =
            Options.key("columns")
                    .type(new TypeReference<List<ColumnConfig>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "The columns to be configured, including nullable and comment");

    public static final Option<List<TableConfig>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableConfig.class)
                    .noDefaultValue()
                    .withDescription("The per-table transform config");

    private String tableComment;
    private List<String> primaryKey;
    private Map<String, ColumnConfig> columnConfigMap;

    public static SinkMapperConfig of(ReadonlyConfig config) {
        String tableComment = config.get(TABLE_COMMENT);
        List<String> primaryKey = config.get(PRIMARY_KEY);
        List<ColumnConfig> columns = config.get(COLUMNS);

        if (columns != null && !columns.isEmpty()) {
            columns.forEach(
                    columnConfig -> {
                        checkArgument(
                                columnConfig.getColumn() != null, "The column name must be set");
                    });
        }

        Map<String, ColumnConfig> columnConfigMap =
                columns != null
                        ? columns.stream()
                                .collect(
                                        Collectors.toMap(
                                                ColumnConfig::getColumn,
                                                columnConfig -> columnConfig))
                        : null;

        return new SinkMapperConfig(tableComment, primaryKey, columnConfigMap);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ColumnConfig implements Serializable {
        private String column;

        @JsonAlias("nullable")
        private Boolean nullable;

        @JsonAlias("comment")
        private String comment;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TableConfig implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        @JsonAlias("table_comment")
        private String tableComment;

        @JsonAlias("primary_key")
        private List<String> primaryKey;

        @JsonAlias("columns")
        private List<ColumnConfig> columns;
    }
}
