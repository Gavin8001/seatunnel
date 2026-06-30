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
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.DbfReadStrategy;

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

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testChineseRoundTripWithGbk() throws Exception {
        // 1. 配置：GBK 编码
        Map<String, Object> writeConfig = createWriteConfig("gbk_roundtrip", "GBK");
        SeaTunnelRowType writeRowType = createIdNameRowType();
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), writeRowType);
        DbfWriteStrategy writeStrategy = new DbfWriteStrategy(writeSinkConfig);

        // 2. 初始化 CatalogTable & Hadoop 本地 FS
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "test_catalog", "test_db", "public", "test_table", writeRowType);
        writeStrategy.setCatalogTable(catalogTable);
        writeStrategy.init(new TestLocalConf(FS_DEFAULT_NAME_DEFAULT), "job-gbk", "prefix-gbk", 0);
        writeStrategy.beginTransaction(1L);

        // 3. 写入 3 条含中文的记录
        Object[][] records = {
            {1, "张三"},
            {2, "李四_王五"},
            {3, "订单#10086_处理中"}
        };
        for (Object[] r : records) {
            writeStrategy.write(new SeaTunnelRow(new Object[] {r[0], r[1]}));
        }

        // 4. 落盘 & 关闭
        writeStrategy.finishAndCloseFile();

        // 5. 计算实际写入的文件路径（getOrCreateFilePathBeingWritten 生成的）
        String actualPath =
                writeStrategy.getOrCreateFilePathBeingWritten(
                        new SeaTunnelRow(new Object[] {1, "placeholder"}));

        // 6. 验证文件存在
        String localPath = actualPath.replaceFirst("^file://", "");
        File outputFile = new File(localPath);
        Assertions.assertTrue(outputFile.exists(), "Output DBF file should exist at " + actualPath);
        Assertions.assertTrue(outputFile.length() > 0, "Output DBF file should not be empty");

        // 7. 用 DbfReadStrategy 以 GBK 读回
        org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf readConf =
                new TestLocalConf(FS_DEFAULT_NAME_DEFAULT);
        org.apache.seatunnel.shade.com.typesafe.config.Config readConfig =
                ConfigFactory.parseMap(java.util.Collections.singletonMap("encoding", "GBK"));
        DbfReadStrategy readStrategy = new DbfReadStrategy();
        readStrategy.setPluginConfig(readConfig);
        readStrategy.init(readConf);
        // Workaround: AbstractReadStrategy.setCatalogTable depends on fileNames being non-empty
        // (it calls fileNames.get(0) internally). We reflectively inject a placeholder to bypass
        // this pre-existing bug, which is independent of the Chinese encoding issue.
        try {
            java.lang.reflect.Field fileNamesField =
                    org.apache.seatunnel.connectors.seatunnel.file.source.reader
                            .AbstractReadStrategy.class
                            .getDeclaredField("fileNames");
            fileNamesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.List<String> fileNames =
                    (java.util.List<String>) fileNamesField.get(readStrategy);
            fileNames.add(localPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up fileNames via reflection", e);
        }
        readStrategy.setCatalogTable(catalogTable);

        final Object collectorLock = new Object();
        java.util.List<SeaTunnelRow> readBack = new java.util.ArrayList<>();
        readStrategy.read(
                actualPath,
                "test_table",
                new org.apache.seatunnel.api.source.Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        readBack.add(record);
                    }

                    @Override
                    public java.lang.Object getCheckpointLock() {
                        return collectorLock;
                    }
                });

        // 8. 断言：3 条全部读回，name 字段与写入值完全相等
        Assertions.assertEquals(3, readBack.size(), "Should read back 3 records");
        for (int i = 0; i < records.length; i++) {
            SeaTunnelRow row = readBack.get(i);
            Assertions.assertEquals(records[i][0], row.getField(0), "id mismatch at row " + i);
            Assertions.assertEquals(
                    records[i][1],
                    row.getField(1),
                    "name mismatch at row "
                            + i
                            + ": expected <"
                            + records[i][1]
                            + "> but was <"
                            + row.getField(1)
                            + ">");
        }

        // 9. 清理
        outputFile.delete();
        // 同时删除空的事务目录
        File parentDir = outputFile.getParentFile();
        while (parentDir != null && parentDir.exists() && parentDir.list().length == 0) {
            parentDir.delete();
            parentDir = parentDir.getParentFile();
        }
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createWriteConfig(String pathSuffix) {
        return createWriteConfig(pathSuffix, null);
    }

    private Map<String, Object> createWriteConfig(String pathSuffix, String encoding) {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/dbf/" + pathSuffix);
        writeConfig.put("file_format_type", FileFormat.DBF.name());
        if (encoding != null) {
            writeConfig.put("encoding", encoding);
        }
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

    private SeaTunnelRowType createIdNameRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
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
