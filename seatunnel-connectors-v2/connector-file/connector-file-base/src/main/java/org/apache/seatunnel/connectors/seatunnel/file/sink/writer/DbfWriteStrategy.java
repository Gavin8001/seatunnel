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
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;

@Slf4j
public class DbfWriteStrategy extends AbstractWriteStrategy<FSDataOutputStream> {

    private final DateUtils.Formatter dateFormatter;
    DateTimeUtils.Formatter dateTimeFormatter;
    private static final int MAX_DBF_STRING_LENGTH = 254;

    private String encoding;
    private String stringLengthStrategy;

    private final LinkedHashMap<String, DBFWriter> beingWrittenDbfWriter;
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;

    public DbfWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenDbfWriter = new LinkedHashMap<>();
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        dateFormatter = fileSinkConfig.getDateFormat();
        dateTimeFormatter = fileSinkConfig.getDatetimeFormat();
        encoding = fileSinkConfig.getEncoding();
        stringLengthStrategy = fileSinkConfig.getDbfStringLengthStrategy();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        getOrCreateOutputStream(filePath);
        DBFWriter dbfWriter = beingWrittenDbfWriter.get(filePath);
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

    private Object convertToDbfValue(Object field, SeaTunnelDataType<?> fieldType, String fieldName)
            throws UnsupportedEncodingException {
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
            if (field instanceof Boolean) {
                return (Boolean) field;
            }
            String strVal = field.toString().toUpperCase();
            return strVal.equals("TRUE")
                    || strVal.equals("T")
                    || strVal.equals("Y")
                    || strVal.equals("1");
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TYPE) {
            if (field instanceof LocalDate) {
                return java.sql.Date.valueOf((LocalDate) field);
            }
            LocalDate parsedDate = DateUtils.parse((String) field, dateFormatter);
            return java.sql.Date.valueOf(parsedDate);
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            if (field instanceof java.time.LocalDateTime) {
                return DateTimeUtils.toString((LocalDateTime) field, dateTimeFormatter);
            }
            return field.toString();
        }

        String strValue;
        if (field instanceof String) {
            strValue = (String) field;
        } else if (field instanceof BigDecimal) {
            strValue = ((BigDecimal) field).toPlainString();
        } else if (field instanceof LocalDate) {
            strValue = DateUtils.toString((LocalDate) field, dateFormatter);
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            strValue = DateTimeUtils.toString((LocalDateTime) field, dateTimeFormatter);
        } else {
            strValue = String.valueOf(field);
        }

        int byteLength = strValue.getBytes(encoding).length;
        if (byteLength > MAX_DBF_STRING_LENGTH) {
            if (FileBaseSinkOptions.DBF_STRING_LENGTH_STRATEGY_TRUNCATE.equals(
                    stringLengthStrategy)) {
                log.warn(
                        "String field '{}' value byte length {} exceeds DBF max length {}, truncating",
                        fieldName,
                        byteLength,
                        MAX_DBF_STRING_LENGTH);
                strValue = truncateString(strValue, MAX_DBF_STRING_LENGTH, encoding);
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

    private String truncateString(String str, int maxBytes, String encoding)
            throws UnsupportedEncodingException {
        byte[] bytes = str.getBytes(encoding);
        if (bytes.length <= maxBytes) {
            return str;
        }
        byte[] truncated = new byte[maxBytes];
        System.arraycopy(bytes, 0, truncated, 0, maxBytes);
        return new String(truncated, encoding);
    }

    @Override
    public FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);

                // Create DBFWriter and fields only once per file
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
                DBFWriter dbfWriter = new DBFWriter(fsDataOutputStream);
                dbfWriter.setFields(fields);
                beingWrittenDbfWriter.put(filePath, dbfWriter);
            } catch (IOException e) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_READ_FAILED,
                        "Failed to create output stream",
                        e);
            }
        }
        return fsDataOutputStream;
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
        beingWrittenDbfWriter.forEach(
                (key, value) -> {
                    try {
                        value.write();
                    } catch (Exception e) {
                        throw new FileConnectorException(
                                FileConnectorErrorCode.FILE_LIST_GET_FAILED,
                                "Failed to write DBF file",
                                e);
                    }
                    try {
                        value.close();
                    } catch (Exception e) {
                        throw new FileConnectorException(
                                FileConnectorErrorCode.FILE_LIST_GET_FAILED,
                                "Failed to close DBF writer",
                                e);
                    }
                });
        beingWrittenDbfWriter.clear();
    }
}
