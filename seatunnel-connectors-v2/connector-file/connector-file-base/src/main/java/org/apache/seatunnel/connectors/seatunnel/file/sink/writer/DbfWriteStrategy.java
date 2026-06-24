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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateUtils;
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
    private DateTimeFormatter configuredDateFormatter;

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
        String encoding = fileSinkConfig.getEncoding();
        if (encoding != null) {
            charset = Charset.forName(encoding);
        }
        // Get configured date format from sink config
        DateUtils.Formatter configuredDateFormat = fileSinkConfig.getDateFormat();
        if (configuredDateFormat != null) {
            configuredDateFormatter = DateTimeFormatter.ofPattern(configuredDateFormat.getValue());
        } else {
            configuredDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        }
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        if (dbfWriter == null) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED,
                    "DbfWriter not initialized. Please call setCatalogTable first.");
        }
        try {
            Object[] record = new Object[seaTunnelRow.getArity()];
            SeaTunnelRowType rowType = seaTunnelRowType;
            SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();

            for (int i = 0; i < seaTunnelRow.getArity(); i++) {
                Object field = seaTunnelRow.getField(i);
                if (field == null) {
                    record[i] = null;
                    continue;
                }

                SeaTunnelDataType<?> fieldType = fieldTypes[i];
                record[i] = convertToDbfValue(field, fieldType, rowType.getFieldName(i));
            }
            dbfWriter.addRecord(record);
        } catch (FileConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED, "Failed to write DBF record", e);
        }
    }

    private Object convertToDbfValue(
            Object field, SeaTunnelDataType<?> fieldType, String fieldName) {
        // For NUMERIC types, keep as actual numbers
        if (fieldType == BasicType.INT_TYPE) {
            if (field instanceof Number) {
                return ((Number) field).intValue();
            }
            return Integer.parseInt(field.toString());
        } else if (fieldType == BasicType.LONG_TYPE) {
            if (field instanceof Number) {
                return ((Number) field).longValue();
            }
            return Long.parseLong(field.toString());
        } else if (fieldType == BasicType.DOUBLE_TYPE || fieldType == BasicType.FLOAT_TYPE) {
            if (field instanceof Number) {
                return ((Number) field).doubleValue();
            }
            return Double.parseDouble(field.toString());
        } else if (fieldType instanceof DecimalType) {
            if (field instanceof BigDecimal) {
                return (BigDecimal) field;
            }
            return new BigDecimal(field.toString());
        } else if (fieldType == BasicType.BOOLEAN_TYPE) {
            // JavaDBF LOGICAL type expects Boolean
            if (field instanceof Boolean) {
                return (Boolean) field;
            }
            String strVal = field.toString().toUpperCase();
            return strVal.equals("TRUE")
                    || strVal.equals("T")
                    || strVal.equals("Y")
                    || strVal.equals("1");
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TYPE) {
            // JavaDBF DATE type expects java.util.Date
            // Use configured date formatter to parse the input date string
            if (field instanceof LocalDate) {
                return java.sql.Date.valueOf((LocalDate) field);
            }
            // Parse using configured formatter, then convert to LocalDate for DBF storage
            LocalDate parsedDate = LocalDate.parse(field.toString(), configuredDateFormatter);
            return java.sql.Date.valueOf(parsedDate);
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            // JavaDBF doesn't support datetime natively, store as timestamp string
            if (field instanceof java.time.LocalDateTime) {
                return ((java.time.LocalDateTime) field).format(DBF_DATE_FORMAT);
            }
            return field.toString();
        }

        // For STRING and other types, convert to String
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

        int byteLength = strValue.getBytes(charset).length;
        if (byteLength > MAX_DBF_STRING_LENGTH) {
            if (FileBaseSinkOptions.DBF_STRING_LENGTH_STRATEGY_TRUNCATE.equals(
                    stringLengthStrategy)) {
                log.warn(
                        "String field '{}' value byte length {} exceeds DBF max length {}, truncating",
                        fieldName,
                        byteLength,
                        MAX_DBF_STRING_LENGTH);
                byte[] bytes = strValue.getBytes(charset);
                byte[] truncated = new byte[MAX_DBF_STRING_LENGTH];
                System.arraycopy(bytes, 0, truncated, 0, MAX_DBF_STRING_LENGTH);
                strValue = new String(truncated, charset);
            } else {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_READ_FAILED,
                        String.format(
                                "String field '%s' value byte length %d exceeds DBF max length %d. "
                                        + "Set 'dbf_string_length_strategy' to 'TRUNCATE' to allow truncation.",
                                fieldName, byteLength, MAX_DBF_STRING_LENGTH));
            }
        }
        return strValue;
    }

    @Override
    public OutputStream getOrCreateOutputStream(String path) throws IOException {
        if (dbfWriter == null) {
            outputStream = hadoopFileSystemProxy.getOutputStream(path);
            SeaTunnelRowType rowType = seaTunnelRowType;
            String[] fieldNames = rowType.getFieldNames();
            SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();

            DBFField[] fields = new DBFField[fieldNames.length];
            for (int i = 0; i < fieldNames.length; i++) {
                DBFField dbffield = new DBFField();
                dbffield.setName(fieldNames[i]);
                DBFDataType dbfType = mapToDbfDataType(fieldTypes[i]);
                dbffield.setType(dbfType);
                dbffield.setFieldLength(getDbfFieldLength(fieldTypes[i]));
                fields[i] = dbffield;
            }
            dbfWriter = new DBFWriter(outputStream);
            dbfWriter.setFields(fields);
        }
        return outputStream;
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
            return 10;
        } else if (fieldType == BasicType.LONG_TYPE) {
            return 20;
        } else if (fieldType == BasicType.DOUBLE_TYPE
                || fieldType == BasicType.FLOAT_TYPE
                || fieldType instanceof DecimalType) {
            return 20;
        } else if (fieldType == BasicType.BOOLEAN_TYPE) {
            return 1;
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TYPE
                || fieldType == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            return 8;
        }
        return MAX_DBF_STRING_LENGTH;
    }

    @Override
    public void finishAndCloseFile() {
        if (dbfWriter != null) {
            try {
                dbfWriter.write();
            } catch (Exception e) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_LIST_GET_FAILED, "Failed to write DBF file", e);
            }
            try {
                dbfWriter.close();
            } catch (Exception e) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_LIST_GET_FAILED,
                        "Failed to close DBF writer",
                        e);
            }
            dbfWriter = null;
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (Exception e) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_LIST_GET_FAILED,
                        "Failed to close output stream",
                        e);
            }
            outputStream = null;
        }
    }
}
