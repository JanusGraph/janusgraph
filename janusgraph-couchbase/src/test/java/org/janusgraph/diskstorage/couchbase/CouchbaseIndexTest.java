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

package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.java.Cluster;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.testutil.JanusGraphCouchbaseContainer;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testcontainers.junit.jupiter.Container;

@Ignore
class CouchbaseIndexTest extends JanusGraphIndexTest {

    @Container
    public static JanusGraphCouchbaseContainer cb = new JanusGraphCouchbaseContainer();

    protected CouchbaseIndexTest() throws InterruptedException {
        super(false, true, true);
    }

    @BeforeAll
    static void beforeAll() {
        cb.start();
    }

    @AfterAll
    static void afterAll() {
        cb.stop();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }

    @Override
    public boolean supportsGeoPointExistsQuery() {
        return true;
    }

    @Override
    public boolean supportsGeoShapePrefixTreeMapping() {
        return true;
    }

    @Override
    public void newTx() {
        super.newTx();
    }

    @Override
    public void clopen(Object... settings) {
        super.clopen(settings);
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @Override
    protected boolean supportsGeoCollections() {
        return false;
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return cb.getJanusGraphConfig();
    }
}
