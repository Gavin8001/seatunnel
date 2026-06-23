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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
public class DbfWriteStrategy extends AbstractWriteStrategy<OutputStream> {

    private static final DateTimeFormatter DBF_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final int MAX_DBF_STRING_LENGTH = 254;

    private Charset charset = StandardCharsets.UTF_8;
    private String stringLengthStrategy = FileBaseSinkOptions.DBF_STRING_LENGTH_STRATEGY_ERROR;
    private DBFWriter dbfWriter;
    private OutputStream outputStream;

    public DbfWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
    }

    @Override
    public void init(
            org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf conf,
            String jobId,
            String uuidPrefix,
            int subTaskIndex) {
        super.init(conf, jobId, uuidPrefix, subTaskIndex);
        // 读取 encoding 配置
        if (pluginConfig.hasPath(FileBaseSinkOptions.ENCODING.key())) {
            String encoding = pluginConfig.getString(FileBaseSinkOptions.ENCODING.key());
            charset = Charset.forName(encoding);
        }
        // 读取字符串长度策略配置
        if (pluginConfig.hasPath(FileBaseSinkOptions.DBF_STRING_LENGTH_STRATEGY.key())) {
            stringLengthStrategy =
                    pluginConfig.getString(FileBaseSinkOptions.DBF_STRING_LENGTH_STRATEGY.key());
        }
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        if (dbfWriter == null) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.WRITER_WRITE_ERROR,
                    "DbfWriter not initialized. Please call setCatalogTable first.");
        }
        try {
            Object[] record = new Object[seaTunnelRow.getArity()];
            CatalogTable catalogTable = getFileSinkConfig().getCatalogTable();
            SeaTunnelDataType<?>[] fieldTypes = catalogTable.getSeaTunnelRowType().getFieldTypes();

            for (int i = 0; i < seaTunnelRow.getArity(); i++) {
                Object field = seaTunnelRow.getField(i);
                if (field == null) {
                    record[i] = null;
                    continue;
                }

                SeaTunnelDataType<?> fieldType = fieldTypes[i];
                String stringValue =
                        convertToString(
                                field,
                                fieldType,
                                catalogTable.getSeaTunnelRowType().getFieldName(i));
                record[i] = stringValue;
            }
            dbfWriter.addRecord(record);
        } catch (FileConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.WRITER_WRITE_ERROR, "Failed to write DBF record", e);
        }
    }

    private String convertToString(Object field, SeaTunnelDataType<?> fieldType, String fieldName) {
        String strValue;
        if (field instanceof String) {
            strValue = (String) field;
        } else if (field instanceof BigDecimal) {
            strValue = ((BigDecimal) field).toPlainString();
        } else if (field instanceof LocalDate) {
            strValue = ((LocalDate) field).format(DBF_DATE_FORMAT);
        } else {
            strValue = String.valueOf(field);
        }

        // 检查字符串长度 (按字节计算，DBF CHAR 字段最多 254 字节)
        int byteLength = strValue.getBytes(charset).length;
        if (byteLength > MAX_DBF_STRING_LENGTH) {
            if (FileBaseSinkOptions.DBF_STRING_LENGTH_STRATEGY_TRUNCATE.equals(
                    stringLengthStrategy)) {
                log.warn(
                        "String field '{}' value byte length {} exceeds DBF max length {}, truncating",
                        fieldName,
                        byteLength,
                        MAX_DBF_STRING_LENGTH);
                // 按字节截断
                byte[] bytes = strValue.getBytes(charset);
                byte[] truncated = new byte[MAX_DBF_STRING_LENGTH];
                System.arraycopy(bytes, 0, truncated, 0, MAX_DBF_STRING_LENGTH);
                strValue = new String(truncated, charset);
            } else {
                throw new FileConnectorException(
                        FileConnectorErrorCode.WRITER_WRITE_ERROR,
                        String.format(
                                "String field '%s' value byte length %d exceeds DBF max length %d. "
                                        + "Set 'dbf_string_length_strategy' to 'TRUNCATE' to allow truncation.",
                                fieldName, byteLength, MAX_DBF_STRING_LENGTH));
            }
        }
        return strValue;
    }

    @Override
    public String generateFileName(String transactionId) {
        return super.generateFileName(transactionId) + ".dbf";
    }

    @Override
    public OutputStream getOrCreateOutputStream(String path) throws IOException {
        if (dbfWriter == null) {
            outputStream = hadoopFileSystemProxy.getOutputStream(path);
            // 构建 DBF 字段定义
            String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            SeaTunnelDataType<?>[] fieldTypes = catalogTable.getSeaTunnelRowType().getFieldTypes();

            dbfWriter = new DBFWriter(outputStream);

            for (int i = 0; i < fieldNames.length; i++) {
                String name = fieldNames[i];
                SeaTunnelDataType<?> type = fieldTypes[i];
                DBFField field = new DBFField();
                field.setName(name);
                DBFDataType dbfType = mapToDbfDataType(type);
                field.setType(dbfType);
                field.setFieldLength(getDbfFieldLength(type));
                dbfWriter.addField(field);
            }
        }
        return null; // DBFWriter handles the actual writing
    }

    private DBFDataType mapToDbfDataType(SeaTunnelDataType<?> fieldType) {
        if (fieldType == BasicType.STRING_TYPE || fieldType == BasicType.VOID_TYPE) {
            return DBFDataType.CHARACTER;
        } else if (fieldType == BasicType.INT_TYPE
                || fieldType == BasicType.LONG_TYPE
                || fieldType instanceof DecimalType
                || fieldType == BasicType.FLOAT_TYPE
                || fieldType == BasicType.DOUBLE_TYPE) {
            return DBFDataType.NUMERIC;
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TYPE
                || fieldType == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            return DBFDataType.DATE;
        } else if (fieldType == BasicType.BOOLEAN_TYPE) {
            return DBFDataType.LOGICAL;
        }
        return DBFDataType.CHARACTER;
    }

    private int getDbfFieldLength(SeaTunnelDataType<?> fieldType) {
        if (fieldType == BasicType.INT_TYPE) {
            return 10; // 9 digits + 1 for sign
        } else if (fieldType == BasicType.LONG_TYPE) {
            return 20; // 18 digits + 1 for sign + 1 for safety margin
        } else if (fieldType == BasicType.DOUBLE_TYPE
                || fieldType == BasicType.FLOAT_TYPE
                || fieldType instanceof DecimalType) {
            return 20;
        } else if (fieldType == BasicType.BOOLEAN_TYPE) {
            return 1;
        }
        return MAX_DBF_STRING_LENGTH;
    }

    @Override
    public void finishAndCloseFile() {
        if (dbfWriter != null) {
            try {
                dbfWriter.close();
            } catch (IOException e) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_OPERATION_FAILED,
                        "Failed to close DBF writer",
                        e);
            }
            dbfWriter = null;
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_OPERATION_FAILED,
                        "Failed to close output stream",
                        e);
            }
            outputStream = null;
        }
    }
}
