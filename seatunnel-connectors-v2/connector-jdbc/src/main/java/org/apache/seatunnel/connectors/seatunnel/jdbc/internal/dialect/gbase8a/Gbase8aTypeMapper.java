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
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Gbase8aTypeMapper implements JdbcDialectTypeMapper {

    private final Gbase8aTypeConverter typeConverter;

    public Gbase8aTypeMapper() {
        this(Gbase8aTypeConverter.INSTANCE);
    }

    public Gbase8aTypeMapper(Gbase8aTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return typeConverter.convert(typeDefine);
    }

    @Override
    @Deprecated
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String gbase8aType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(metadata.getColumnName(colIndex))
                        .columnType(gbase8aType)
                        .dataType(gbase8aType)
                        .nullable(metadata.isNullable(colIndex) == ResultSetMetaData.columnNullable)
                        .precision((long) precision)
                        .scale(scale)
                        .build();

        return mappingColumn(typeDefine).getDataType();
    }
}
