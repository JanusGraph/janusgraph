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

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.indexing.*;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.*;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;

import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

import static org.junit.Assert.*;

/**
 * Test behavior JanusGraph ConfigOptions governing ES client setup.
 */
public class ElasticSearchConfigTest {

    private static final String INDEX_NAME = "escfg";

    private ElasticsearchRunner esr;

    private int port;

    @Before
    public void setup() throws Exception {
        esr = new ElasticsearchRunner();
        esr.start();
        port = getInterface() == ElasticSearchSetup.REST_CLIENT ? 9200 : 9300;
    }

    @After
    public void teardown() throws Exception {
        esr.stop();
    }

    public ElasticSearchSetup getInterface() {
        return ElasticSearchSetup.REST_CLIENT;
    }

    @Test
    public void testJanusGraphFactoryBuilder() {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "inmemory");
        builder.set("index." + INDEX_NAME + ".elasticsearch.hostname", "127.0.0.1:" + port);
        JanusGraph graph = builder.open(); // Must not throw an exception
        assertTrue(graph.isOpen());
        graph.close();
    }

    @Test
    public void testClient() throws BackendException, InterruptedException {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, getInterface().toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1:" + port }, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, getInterface().toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "10.11.12.13:" + port }, INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);
        Throwable failure = null;
        try {
            idx = new ElasticSearchIndex(indexConfig);
        } catch (Throwable t) {
            failure = t;
        }
        // idx.close();
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);
    }

    @Test
    public void testIndexCreationOptions() throws InterruptedException, BackendException, IOException {
        final int shards = 7;

        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.create.ext.number_of_shards", String.valueOf(shards));
        ModifiableConfiguration config =
            new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, getInterface().toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1:" + port }, INDEX_NAME);
        config.set(GraphDatabaseConfiguration.INDEX_NAME, "janusgraph_creation_opts", INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);

        ElasticSearchClient client = getInterface().connect(indexConfig).getClient();

        assertEquals(String.valueOf(shards), client.getIndexSettings("janusgraph_creation_opts").get("number_of_shards"));

        idx.close();
        client.close();
    }

    private void simpleWriteAndQuery(IndexProvider idx) throws BackendException, InterruptedException {

        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "jvmlocal_test_store";
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest.getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), "standard", "keyword"));

        BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        assertEquals(0, itx.query(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "ali"))).size());
        itx.add(storeName, "doc", IndexProviderTest.NAME, "alice", false);
        itx.commit();
        Thread.sleep(1500L); // Slightly longer than default 1s index.refresh_interval
        itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        assertEquals(0, itx.query(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "zed"))).size());
        assertEquals(1, itx.query(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "ali"))).size());
        itx.rollback();
    }
}
