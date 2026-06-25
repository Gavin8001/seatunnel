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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Gbase8aTypeConverterTest {

    @Test
    public void testIdentifier() {
        Assertions.assertEquals("Gbase8a", Gbase8aTypeConverter.INSTANCE.identifier());
    }

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testReconvertUnsupported() {
        Column column =
                PhysicalColumn.of(
                        "test",
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        (Long) null,
                        true,
                        null,
                        null);
        try {
            Gbase8aTypeConverter.INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertTinyint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TINYINT")
                        .dataType("TINYINT")
                        .build();
        Column column = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals("TINYINT", column.getSourceType());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = Gbase8aTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(Gbase8aTypeConverter.GBASE8A_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(Gbase8aTypeConverter.GBASE8A_TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testConvertDecimalPreserved() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DECIMAL(10,2)")
                        .dataType("DECIMAL")
                        .precision(10L)
                        .scale(2)
                        .build();
        Column column = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals("DECIMAL(10,2)", column.getSourceType());
    }

    @Test
    public void testConvertDecimalPrecisionBoundary() {
        // precision >= 38 should fall back to DecimalType(38, 18)
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DECIMAL(50,10)")
                        .dataType("DECIMAL")
                        .precision(50L)
                        .scale(10)
                        .build();
        Column column = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(38, column.getColumnLength());
        Assertions.assertEquals(18, column.getScale());
        Assertions.assertEquals("DECIMAL(38,18)", column.getSourceType());
    }

    @Test
    public void testConvertVarchar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARCHAR(255)")
                        .dataType("VARCHAR")
                        .length(255L)
                        .build();
        Column column = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(255L, column.getColumnLength());
        Assertions.assertEquals("VARCHAR(255)", column.getSourceType());
    }

    @Test
    public void testReconvertVarcharWithLength() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .build();

        BasicTypeDefine typeDefine = Gbase8aTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("VARCHAR(%s)", column.getColumnLength()), typeDefine.getColumnType());
        Assertions.assertEquals(Gbase8aTypeConverter.GBASE8A_VARCHAR, typeDefine.getDataType());
        Assertions.assertEquals(column.getColumnLength(), typeDefine.getLength());
    }

    @Test
    public void testReconvertStringWithNoLength() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = Gbase8aTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(Gbase8aTypeConverter.GBASE8A_TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(Gbase8aTypeConverter.GBASE8A_TEXT, typeDefine.getDataType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("DATE").dataType("DATE").build();
        Column column = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals("DATE", column.getSourceType());
    }

    @Test
    public void testConvertTimestamp() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP")
                        .dataType("TIMESTAMP")
                        .build();
        Column column = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals("TIMESTAMP", column.getSourceType());
    }
}
