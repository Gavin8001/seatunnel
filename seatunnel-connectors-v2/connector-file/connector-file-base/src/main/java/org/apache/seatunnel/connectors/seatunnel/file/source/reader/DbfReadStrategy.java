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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;

@Slf4j
public class DbfReadStrategy extends AbstractReadStrategy {

    private DateUtils.Formatter dateFormatter =
            FileBaseSourceOptions.DATE_FORMAT_LEGACY.defaultValue();

    private DateTimeUtils.Formatter dateTimeFormatter =
            FileBaseSourceOptions.DATETIME_FORMAT_LEGACY.defaultValue();

    private String encoding = FileBaseSourceOptions.ENCODING.defaultValue();

    @Override
    public void init(org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(FileBaseSourceOptions.ENCODING.key())) {
            encoding = pluginConfig.getString(FileBaseSourceOptions.ENCODING.key());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.DATE_FORMAT_LEGACY.key())) {
            dateFormatter =
                    pluginConfig.getEnum(
                            DateUtils.Formatter.class,
                            FileBaseSourceOptions.DATE_FORMAT_LEGACY.key());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.DATETIME_FORMAT_LEGACY.key())) {
            dateTimeFormatter =
                    pluginConfig.getEnum(
                            DateTimeUtils.Formatter.class,
                            FileBaseSourceOptions.DATETIME_FORMAT_LEGACY.key());
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
            DBFReader reader = new DBFReader(inputStream, Charset.forName(encoding));
            SeaTunnelRowType rowType = getSeaTunnelRowType();
            int fieldCount = rowType.getTotalFields();

            DBFRow record;
            while ((record = reader.nextRow()) != null) {
                Object[] fields = new Object[fieldCount];
                for (int i = 0; i < fieldCount; i++) {
                    fields[i] = extractFieldValue(record, i, rowType.getFieldType(i));
                }
                SeaTunnelRow row = new SeaTunnelRow(fields);
                row.setTableId(tableId);
                output.collect(row);
            }
        } catch (FileConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED, "Failed to read DBF file: " + path, e);
        }
    }

    private Object extractFieldValue(
            DBFRow record, int fieldIndex, SeaTunnelDataType<?> fieldType) {
        Object value = record.getObject(fieldIndex);
        if (value == null) {
            return null;
        }
        String strValue = value.toString().trim();
        if (strValue.isEmpty()) {
            return null;
        }
        if (fieldType == BasicType.STRING_TYPE) {
            return strValue;
        } else if (fieldType == BasicType.INT_TYPE) {
            return Integer.parseInt(strValue);
        } else if (fieldType == BasicType.LONG_TYPE) {
            return Long.parseLong(strValue);
        } else if (fieldType == BasicType.DOUBLE_TYPE) {
            return Double.parseDouble(strValue);
        } else if (fieldType == BasicType.BOOLEAN_TYPE) {
            String v = strValue.toUpperCase();
            return v.equals("Y") || v.equals("T") || v.equals("1");
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TYPE) {
            return DateUtils.parse(strValue, dateFormatter);
        } else if (fieldType == LocalTimeType.LOCAL_DATE_TIME_TYPE) {
            return DateTimeUtils.parse(strValue, dateTimeFormatter);
        } else if (fieldType instanceof DecimalType) {
            return new BigDecimal(strValue);
        }
        return strValue;
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return getSeaTunnelRowType();
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(TablePath tablePath, String path)
            throws FileConnectorException {
        return getSeaTunnelRowType();
    }

    private SeaTunnelRowType getSeaTunnelRowType() {
        if (seaTunnelRowType != null) {
            return seaTunnelRowType;
        }
        throw new UnsupportedOperationException(
                "Auto-inferring DBF schema from file header is not yet implemented. "
                        + "Please provide CatalogTable schema configuration.");
    }

    @Override
    public void setCatalogTable(org.apache.seatunnel.api.table.catalog.CatalogTable catalogTable) {
        super.setCatalogTable(catalogTable);
    }
}
