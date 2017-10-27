// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.es;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.janusgraph.CassandraStorageSetup.getCassandraThriftConfiguration;

public class ThriftElasticsearchTest extends JanusGraphIndexTest {

    private static ElasticsearchRunner esr;

    @BeforeClass
    public static void startElasticsearch() {
        CassandraStorageSetup.startCleanEmbedded();
        esr = new ElasticsearchRunner();
        esr.start();
    }

    @AfterClass
    public static void stopElasticsearch() {
        esr.stop();
    }

    public ThriftElasticsearchTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getCassandraThriftConfiguration(ThriftElasticsearchTest.class.getName());
        return esr.setElasticsearchConfiguration(config, INDEX)
            .set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, INDEX)
            .getConfiguration();
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
    protected boolean supportsCollections() {
        return true;
    }

}
