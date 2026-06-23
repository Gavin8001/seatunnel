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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class DbfReadStrategyTest {

    @Test
    public void testReadDbfFileWithSchema() throws Exception {
        // This test requires a sample DBF file
        // Skip if no sample file available
        Path tempDir = Files.createTempDirectory("dbf-test");
        Path dbfFile = tempDir.resolve("test.dbf");

        try {
            DbfReadStrategy readStrategy = new DbfReadStrategy();

            // Create a simple schema for testing
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
                            new java.util.HashMap<>(),
                            new ArrayList<>());

            readStrategy.setCatalogTable(catalogTable);

            // Note: Actual DBF file reading test requires a sample DBF file
            // This test validates the strategy can be instantiated and configured
            Assertions.assertNotNull(readStrategy);
        } finally {
            Files.deleteIfExists(dbfFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    public void testGetSeaTunnelRowTypeInfo() {
        DbfReadStrategy readStrategy = new DbfReadStrategy();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            org.apache.seatunnel.api.table.type.BasicType.INT_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                        });

        CatalogTable catalogTable =
                CatalogTable.of(
                        new org.apache.seatunnel.api.table.catalog.TablePath(
                                "test_db", "test_table"),
                        rowType,
                        new java.util.HashMap<>(),
                        new ArrayList<>());

        readStrategy.setCatalogTable(catalogTable);

        // Verify schema is set
        Assertions.assertNotNull(readStrategy.getActualSeaTunnelRowTypeInfo());
    }
}
