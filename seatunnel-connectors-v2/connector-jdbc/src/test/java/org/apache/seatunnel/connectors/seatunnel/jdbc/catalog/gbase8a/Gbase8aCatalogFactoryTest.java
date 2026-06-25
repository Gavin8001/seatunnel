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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.gbase8a;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Gbase8aCatalogFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        Gbase8aCatalogFactory factory = new Gbase8aCatalogFactory();
        Assertions.assertEquals("Gbase8a", factory.factoryIdentifier());
    }

    @Test
    public void testCreateCatalogWithUrl() {
        Gbase8aCatalogFactory factory = new Gbase8aCatalogFactory();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:gbase://localhost:5258/test");
        configMap.put("username", "test_user");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        Catalog catalog = factory.createCatalog("test_catalog", config);
        Assertions.assertNotNull(catalog);
        Assertions.assertInstanceOf(Gbase8aCatalog.class, catalog);
    }

    @Test
    public void testCreateCatalogWithoutUrlThrows() {
        Gbase8aCatalogFactory factory = new Gbase8aCatalogFactory();
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.emptyMap());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> factory.createCatalog("test_catalog", config));
    }
}
