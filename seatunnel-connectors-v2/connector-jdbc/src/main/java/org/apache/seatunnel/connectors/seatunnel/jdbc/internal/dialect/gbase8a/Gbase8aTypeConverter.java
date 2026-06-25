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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gbase8a;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class Gbase8aTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ref http://www.gbase.cn/down/4419.html
    // ============================data types=====================

    // -------------------------number----------------------------
    public static final String GBASE8A_TINYINT = "TINYINT";
    public static final String GBASE8A_SMALLINT = "SMALLINT";
    public static final String GBASE8A_INT = "INT";
    public static final String GBASE8A_BIGINT = "BIGINT";
    public static final String GBASE8A_DECIMAL = "DECIMAL";
    public static final String GBASE8A_FLOAT = "FLOAT";
    public static final String GBASE8A_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    public static final String GBASE8A_CHAR = "CHAR";
    public static final String GBASE8A_VARCHAR = "VARCHAR";

    // ------------------------------time-------------------------
    public static final String GBASE8A_DATE = "DATE";
    public static final String GBASE8A_TIME = "TIME";
    public static final String GBASE8A_TIMESTAMP = "TIMESTAMP";
    public static final String GBASE8A_DATETIME = "DATETIME";

    // ------------------------------blob-------------------------
    public static final String GBASE8A_BLOB = "BLOB";
    public static final String GBASE8A_TEXT = "TEXT";

    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 18;

    public static final Gbase8aTypeConverter INSTANCE = new Gbase8aTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.GBASE_8A;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String gbaseType = typeDefine.getDataType().toUpperCase();
        switch (gbaseType) {
            case GBASE8A_TINYINT:
                builder.sourceType(GBASE8A_TINYINT).dataType(BasicType.BYTE_TYPE);
                break;
            case GBASE8A_SMALLINT:
                builder.sourceType(GBASE8A_SMALLINT).dataType(BasicType.SHORT_TYPE);
                break;
            case GBASE8A_INT:
                builder.sourceType(GBASE8A_INT).dataType(BasicType.INT_TYPE);
                break;
            case GBASE8A_BIGINT:
                builder.sourceType(GBASE8A_BIGINT).dataType(BasicType.LONG_TYPE);
                break;
            case GBASE8A_DECIMAL:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null
                        && typeDefine.getPrecision() > 0
                        && typeDefine.getPrecision() < DEFAULT_PRECISION) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.sourceType(
                        String.format(
                                "DECIMAL(%s,%s)",
                                decimalType.getPrecision(), decimalType.getScale()));
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case GBASE8A_FLOAT:
                builder.sourceType(GBASE8A_FLOAT).dataType(BasicType.FLOAT_TYPE);
                break;
            case GBASE8A_DOUBLE:
                builder.sourceType(GBASE8A_DOUBLE).dataType(BasicType.DOUBLE_TYPE);
                break;
            case GBASE8A_CHAR:
            case GBASE8A_VARCHAR:
                builder.sourceType(String.format("%s(%s)", gbaseType, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case GBASE8A_DATE:
                builder.sourceType(GBASE8A_DATE).dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case GBASE8A_TIME:
                builder.sourceType(GBASE8A_TIME).dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case GBASE8A_TIMESTAMP:
            case GBASE8A_DATETIME:
                builder.sourceType(gbaseType).dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case GBASE8A_BLOB:
            case GBASE8A_TEXT:
                builder.sourceType(gbaseType)
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(typeDefine.getLength());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.GBASE_8A,
                        typeDefine.getDataType(),
                        typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case TINYINT:
                builder.columnType(GBASE8A_TINYINT).dataType(GBASE8A_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(GBASE8A_SMALLINT).dataType(GBASE8A_SMALLINT);
                break;
            case INT:
                builder.columnType(GBASE8A_INT).dataType(GBASE8A_INT);
                break;
            case BIGINT:
                builder.columnType(GBASE8A_BIGINT).dataType(GBASE8A_BIGINT);
                break;
            case FLOAT:
                builder.columnType(GBASE8A_FLOAT).dataType(GBASE8A_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(GBASE8A_DOUBLE).dataType(GBASE8A_DOUBLE);
                break;
            case DECIMAL:
                DecimalType dt = (DecimalType) column.getDataType();
                int precision = dt.getPrecision();
                int scale = dt.getScale();
                builder.columnType(String.format("DECIMAL(%s,%s)", precision, scale))
                        .dataType(GBASE8A_DECIMAL)
                        .precision((long) precision)
                        .scale(scale);
                break;
            case STRING:
                Long len = column.getColumnLength();
                if (len == null || len <= 0) {
                    builder.columnType(GBASE8A_TEXT).dataType(GBASE8A_TEXT);
                } else {
                    builder.columnType(String.format("VARCHAR(%s)", len))
                            .dataType(GBASE8A_VARCHAR)
                            .length(len);
                }
                break;
            case BYTES:
                builder.columnType(GBASE8A_BLOB).dataType(GBASE8A_BLOB);
                break;
            case DATE:
                builder.columnType(GBASE8A_DATE).dataType(GBASE8A_DATE);
                break;
            case TIME:
                builder.columnType(GBASE8A_TIME).dataType(GBASE8A_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(GBASE8A_TIMESTAMP).dataType(GBASE8A_TIMESTAMP);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.GBASE_8A,
                        column.getDataType().toString(),
                        column.getName());
        }
        return builder.build();
    }
}
