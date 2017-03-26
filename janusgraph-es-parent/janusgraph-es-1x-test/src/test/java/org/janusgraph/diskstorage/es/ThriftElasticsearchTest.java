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

import java.util.concurrent.ExecutionException;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.es.ThriftEsShim;
import org.janusgraph.testcategory.BrittleTests;
import org.janusgraph.testutil.TestGraphConfigs;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.janusgraph.CassandraStorageSetup.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

public class ThriftElasticsearchTest extends JanusGraphIndexTest {

    public static final String ES_HOME = "../../janusgraph-es-parent/janusgraph-es";

    public ThriftElasticsearchTest() {
        super(true, true, true);
    }

    @BeforeClass
    public static void beforeThriftTest() {
        CassandraStorageSetup.startCleanEmbedded();
        new ElasticsearchRunner(ES_HOME).start();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                getCassandraThriftConfiguration(ThriftElasticsearchTest.class.getName());
        //Add index
        config.set(INDEX_BACKEND,"elasticsearch",INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("es"),INDEX);
        return config.getConfiguration();
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

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
