/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.graphdb;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.testutil.JanusGraphCouchbaseContainer;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Testcontainers
public class CouchbaseGraphTest extends JanusGraphTest {
    @Container
    public static JanusGraphCouchbaseContainer cb = new JanusGraphCouchbaseContainer();

    @BeforeAll
    public static void beforeAll() {
        cb.start();
    }

    @AfterAll
    public static void afterAll() {
        cb.stop();
    }

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return cb.getJanusGraphConfig();
    }

    @Override
    @Ignore // test uses elasticsearch as indexing backend despite provided configuration, which results in ClassDefNotFoundError.
    public void testSimpleJsonSchemaImportFromProperty() throws IOException, BackendException {
        super.testSimpleJsonSchemaImportFromProperty();
    }



    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1457
    @Disabled
    public void simpleLogTest(boolean useStringId) throws InterruptedException {
        super.simpleLogTest(useStringId);
    }


    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1457
    @Disabled
    public void simpleLogTestWithFailure(boolean useStringId) throws InterruptedException {
        super.simpleLogTestWithFailure(useStringId);
    }

    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1498
    @Disabled
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException {
        super.testIndexUpdatesWithReindexAndRemove();
    }


    @Override @Test
    @Disabled
    public void testLimitBatchSizeForMultiQuery() {}

    @Override @Test
    @Disabled
    public void testGotGLoadWithoutIndexBackendException() {}
}
