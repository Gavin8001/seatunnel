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

package org.apache.seatunnel.e2e.connector.file.local;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.TestTemplate;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * DBF file format integration test.
 *
 * <p>Tests DBF file read/write with various field types including STRING, INT, DOUBLE, BOOLEAN, and
 * DATE.
 */
@Slf4j
public class DbfFileIT extends TestSuiteBase {

    /** Copy DBF test files to container */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                ContainerUtil.copyFileIntoContainers(
                        "/dbf/e2e.dbf",
                        "/seatunnel/read/dbf/name=tyrantlucifer/hobby=coding/e2e.dbf",
                        container);
            };

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Only support for seatunnel")
    public void testLocalDbfFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);

        // test write local dbf file
        helper.execute("/dbf/fake_to_local_dbf.conf");
        // test read local dbf file
        helper.execute("/dbf/local_dbf_to_assert.conf");

        // test write local dbf file with GBK encoding
        helper.execute("/dbf/fake_to_local_dbf_gbk.conf");
        // test read local dbf file with GBK encoding
        helper.execute("/dbf/local_dbf_gbk_to_assert.conf");
    }
}
