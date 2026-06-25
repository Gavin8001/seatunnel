/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Gbase8aTypeMapperTest {

    @Test
    public void testMappingColumnDelegatesToTypeConverter() {
        Gbase8aTypeMapper mapper = new Gbase8aTypeMapper();
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("id")
                        .columnType("BIGINT")
                        .dataType("BIGINT")
                        .nullable(false)
                        .build();

        Column result = mapper.mappingColumn(typeDefine);
        Column expected = Gbase8aTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(expected.getName(), result.getName());
        Assertions.assertEquals(expected.getDataType(), result.getDataType());
        Assertions.assertEquals(expected.getSourceType(), result.getSourceType());
        Assertions.assertEquals(expected.isNullable(), result.isNullable());
        Assertions.assertEquals(BasicType.LONG_TYPE, result.getDataType());
    }

    @Test
    public void testConstructorInjectsCustomConverter() {
        Gbase8aTypeConverter customConverter = Gbase8aTypeConverter.INSTANCE;
        Gbase8aTypeMapper mapper = new Gbase8aTypeMapper(customConverter);

        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("name")
                        .columnType("VARCHAR(64)")
                        .dataType("VARCHAR")
                        .length(64L)
                        .build();

        Column result = mapper.mappingColumn(typeDefine);

        Assertions.assertEquals("name", result.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, result.getDataType());
        Assertions.assertEquals("VARCHAR(64)", result.getSourceType());
        Assertions.assertEquals(64L, result.getColumnLength());
    }
}
