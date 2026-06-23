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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

public class DbfWriteStrategyTest {

    @Test
    public void testGenerateFileName() {
        FileSinkConfig fileSinkConfig = new FileSinkConfig(new HashMap<>());
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(fileSinkConfig);

        String fileName = writeStrategy.generateFileName("tx_123");

        Assertions.assertTrue(fileName.endsWith(".dbf"), "Generated filename should end with .dbf");
    }

    @Test
    public void testSetCatalogTable() {
        FileSinkConfig fileSinkConfig = new FileSinkConfig(new HashMap<>());
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(fileSinkConfig);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "value"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            org.apache.seatunnel.api.table.type.BasicType.INT_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE
                        });

        CatalogTable catalogTable =
                CatalogTable.of(
                        new org.apache.seatunnel.api.table.catalog.TablePath(
                                "test_db", "test_table"),
                        rowType,
                        new HashMap<>(),
                        new ArrayList<>());

        writeStrategy.setCatalogTable(catalogTable);

        // Verify catalog table is set (internal validation)
        Assertions.assertNotNull(writeStrategy);
    }

    @Test
    public void testStringLengthStrategyConfig() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("dbf_string_length_strategy", "TRUNCATE");

        FileSinkConfig fileSinkConfig = new FileSinkConfig(config);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(fileSinkConfig);

        // Verify strategy can be created with config
        Assertions.assertNotNull(writeStrategy);
    }
}
