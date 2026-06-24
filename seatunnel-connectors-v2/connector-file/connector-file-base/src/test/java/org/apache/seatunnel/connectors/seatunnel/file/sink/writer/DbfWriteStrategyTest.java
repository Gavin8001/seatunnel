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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class DbfWriteStrategyTest {

    private static final String TMP_PATH = "file:///tmp/seatunnel/dbf/test";

    @Test
    public void testDbfWriteStrategyCanBeInstantiated() {
        Map<String, Object> writeConfig = createWriteConfig("test");
        SeaTunnelRowType rowType = createSimpleRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        Assertions.assertNotNull(writeStrategy);
    }

    @Test
    public void testGenerateFileName() {
        Map<String, Object> writeConfig = createWriteConfig("generate");
        SeaTunnelRowType rowType = createSimpleRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        String fileName = writeStrategy.generateFileName("tx_123");
        Assertions.assertTrue(fileName.endsWith(".dbf"), "Generated filename should end with .dbf");
    }

    @Test
    public void testStringLengthStrategyConfigTruncate() {
        Map<String, Object> writeConfig = createWriteConfig("truncate");
        writeConfig.put("dbf_string_length_strategy", "TRUNCATE");
        SeaTunnelRowType rowType = createSimpleRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        Assertions.assertNotNull(writeStrategy);
    }

    @Test
    public void testStringLengthStrategyConfigError() {
        Map<String, Object> writeConfig = createWriteConfig("error");
        writeConfig.put("dbf_string_length_strategy", "ERROR");
        SeaTunnelRowType rowType = createSimpleRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        Assertions.assertNotNull(writeStrategy);
    }

    @Test
    public void testSetCatalogTable() {
        Map<String, Object> writeConfig = createWriteConfig("catalog");
        SeaTunnelRowType rowType = createThreeFieldRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "test_catalog", "test_db", "public", "test_table", rowType);
        writeStrategy.setCatalogTable(catalogTable);
        Assertions.assertNotNull(writeStrategy);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testWriteAndReadWithAllSupportedDataTypes() throws Exception {
        Map<String, Object> writeConfig = createWriteConfig("all_types");
        SeaTunnelRowType writeRowType = createAllTypesRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), writeRowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "test_catalog", "test_db", "public", "test_table", writeRowType);
        writeStrategy.setCatalogTable(catalogTable);
        // Initialize with local file system configuration
        writeStrategy.init(new TestLocalConf(FS_DEFAULT_NAME_DEFAULT), "job123", "prefix", 0);
        // Get output stream to initialize writer
        String testPath = "/tmp/seatunnel/dbf/all_types/test.dbf";
        writeStrategy.getOrCreateOutputStream(testPath);
        // Write a row with all supported data types
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            1, // INT
                            "Test User", // STRING
                            99.99, // DOUBLE
                            true, // BOOLEAN
                            LocalDate.of(2024, 1, 15), // DATE
                            new BigDecimal("1234.56") // DECIMAL
                        });
        writeStrategy.write(row);
        // Finish and close file
        writeStrategy.finishAndCloseFile();
        // Verify file was created
        File outputFile = new File(testPath);
        Assertions.assertTrue(outputFile.exists(), "Output DBF file should exist");
        // Clean up
        if (outputFile.exists()) {
            outputFile.delete();
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testWriteWithCustomDateFormat() throws Exception {
        // Test with custom date format "yyyy-MM-dd" instead of default "yyyyMMdd"
        Map<String, Object> writeConfig = createWriteConfig("custom_date");
        writeConfig.put("date_format", "yyyy-MM-dd");
        SeaTunnelRowType writeRowType = createDateOnlyRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), writeRowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "test_catalog", "test_db", "public", "test_table", writeRowType);
        writeStrategy.setCatalogTable(catalogTable);
        // Initialize with local file system configuration
        writeStrategy.init(new TestLocalConf(FS_DEFAULT_NAME_DEFAULT), "job123", "prefix", 0);
        // Get output stream to initialize writer
        String testPath = "/tmp/seatunnel/dbf/custom_date/test.dbf";
        writeStrategy.getOrCreateOutputStream(testPath);
        // Write a row with custom date format
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, LocalDate.of(2024, 1, 15)});
        writeStrategy.write(row);
        // Finish and close file
        writeStrategy.finishAndCloseFile();
        // Verify file was created
        File outputFile = new File(testPath);
        Assertions.assertTrue(
                outputFile.exists(), "Output DBF file should exist with custom date format");
        // Clean up
        if (outputFile.exists()) {
            outputFile.delete();
        }
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createWriteConfig(String pathSuffix) {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/dbf/" + pathSuffix);
        writeConfig.put("file_format_type", FileFormat.DBF.name());
        return writeConfig;
    }

    private SeaTunnelRowType createSimpleRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
    }

    private SeaTunnelRowType createThreeFieldRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name", "value"},
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE
                });
    }

    private SeaTunnelRowType createAllTypesRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name", "price", "is_active", "create_dt", "balance"},
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.DOUBLE_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    LocalTimeType.LOCAL_DATE_TYPE,
                    new DecimalType(10, 2)
                });
    }

    private SeaTunnelRowType createDateOnlyRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "cdate"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TYPE});
    }

    /** Test configuration for local file system */
    private static class TestLocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public TestLocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
