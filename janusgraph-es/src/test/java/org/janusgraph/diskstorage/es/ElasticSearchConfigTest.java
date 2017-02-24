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

import com.google.common.base.Joiner;

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
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;

import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CONF_FILE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

import static org.junit.Assert.*;

/**
 * Test behavior JanusGraph ConfigOptions governing ES client setup.
 *
 * {@link ElasticSearchIndexTest#testConfiguration()} exercises legacy
 * config options using an embedded JVM-local-transport ES instance.  By contrast,
 * this class exercises the new {@link ElasticSearchIndex#INTERFACE} configuration
 * mechanism and uses a network-capable embedded ES instance.
 */
public class ElasticSearchConfigTest {

    private static final String INDEX_NAME = "escfg";

    @BeforeClass
    public static void killElasticsearch() {
        IOUtils.deleteDirectory(new File("es"), true);
        ElasticsearchRunner esr = new ElasticsearchRunner();
        esr.stop();
    }

    @Before
    public void setup() throws IOException {
        String baseDir = Joiner.on(File.separator).join("target", "es");
        FileUtils.deleteDirectory(new File(baseDir + File.separator + "data"));
    }

    @Test
    public void testJanusGraphFactoryBuilder()
    {
        String baseDir = Joiner.on(File.separator).join("target", "es");
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "inmemory");
        builder.set("index." + INDEX_NAME + ".elasticsearch.interface", "NODE");
        builder.set("index." + INDEX_NAME + ".elasticsearch.ext.node.data", "true");
        builder.set("index." + INDEX_NAME + ".elasticsearch.ext.node.client", "false");
        builder.set("index." + INDEX_NAME + ".elasticsearch.ext.node.local", "true");
        builder.set("index." + INDEX_NAME + ".elasticsearch.ext.path.home", baseDir);
        JanusGraph graph = builder.open(); // Must not throw an exception
        assertTrue(graph.isOpen());
        graph.close();
    }

    @Test
    public void testTransportClient() throws BackendException, InterruptedException {
        ElasticsearchRunner esr = new ElasticsearchRunner(".");
        esr.start();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.TRANSPORT_CLIENT.toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1" }, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.TRANSPORT_CLIENT.toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "10.11.12.13" }, INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);
        Throwable failure = null;
        try {
            idx = new ElasticSearchIndex(indexConfig);
        } catch (Throwable t) {
            failure = t;
        }
        // idx.close();
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);

        esr.stop();
    }

    @Test
    public void testLocalNodeUsingExt() throws BackendException, InterruptedException {

        String baseDir = Joiner.on(File.separator).join("target", "es");

        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.data", "true");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.client", "false");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.local", "true");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.path.home", baseDir);
        ModifiableConfiguration config =
                new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                        cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        assertTrue(new File(baseDir + File.separator + "data").exists());
    }

    @Test
    public void testLocalNodeUsingExtAndIndexDirectory() throws BackendException, InterruptedException {

        String baseDir = Joiner.on(File.separator).join("target", "es");

        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.data", "true");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.client", "false");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.local", "true");
        ModifiableConfiguration config =
                new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                        cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        config.set(INDEX_DIRECTORY, baseDir, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        assertTrue(new File(baseDir + File.separator + "data").exists());
    }

    @Test
    public void testLocalNodeUsingYaml() throws BackendException, InterruptedException {

        String baseDir = Joiner.on(File.separator).join("target", "es");

        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        config.set(INDEX_CONF_FILE,
                Joiner.on(File.separator).join("target", "test-classes", "es_jvmlocal.yml"), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        assertTrue(new File(baseDir + File.separator + "data").exists());
    }

    @Test
    public void testNetworkNodeUsingExt() throws BackendException, InterruptedException {
        ElasticsearchRunner esr = new ElasticsearchRunner(".");
        esr.start();
        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.data", "false");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.client", "true");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.discovery.zen.ping.multicast.enabled", "false");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.discovery.zen.ping.unicast.hosts", "localhost,127.0.0.1:9300");
        ModifiableConfiguration config =
                new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.discovery.zen.ping.unicast.hosts", "10.11.12.13");
        config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                 cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        config.set(HEALTH_REQUEST_TIMEOUT, "5s", INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);

        Throwable failure = null;
        try {
            idx = new ElasticSearchIndex(indexConfig);
        } catch (Throwable t) {
            failure = t;
        }
        // idx.close();
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);
        esr.stop();
    }

    @Test
    public void testNetworkNodeUsingYaml() throws BackendException, InterruptedException {
        ElasticsearchRunner esr = new ElasticsearchRunner(".");
        esr.start();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        config.set(INDEX_CONF_FILE,
                Joiner.on(File.separator).join("target", "test-classes", "es_cfg_nodeclient.yml"), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        config.set(HEALTH_REQUEST_TIMEOUT, "5s", INDEX_NAME);
        config.set(INDEX_CONF_FILE,
                Joiner.on(File.separator).join("target", "test-classes", "es_cfg_bogus_nodeclient.yml"), INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);

        Throwable failure = null;
        try {
            idx = new ElasticSearchIndex(indexConfig);
        } catch (Throwable t) {
            failure = t;
        }
        //idx.close();
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);
        esr.stop();
    }

    @Test
    public void testIndexCreationOptions() throws InterruptedException, BackendException {

        String baseDir = Joiner.on(File.separator).join("target", "es");

        final int shards = 7;

        ElasticsearchRunner esr = new ElasticsearchRunner(".");
        esr.start();
        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.create.ext.number_of_shards", String.valueOf(shards));
        ModifiableConfiguration config =
                new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                        cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE.toString(), INDEX_NAME);
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, baseDir, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        simpleWriteAndQuery(idx);



        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put("discovery.zen.ping.multicast.enabled", "false");
        settingsBuilder.put("discovery.zen.ping.unicast.hosts", "localhost,127.0.0.1:9300");
        settingsBuilder.put("path.home", baseDir);
        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().settings(settingsBuilder.build());
        nodeBuilder.client(true).data(false).local(false);
        Node n = nodeBuilder.build().start();

        GetSettingsResponse response = n.client().admin().indices().getSettings(new GetSettingsRequest().indices("janusgraph")).actionGet();
        assertEquals(String.valueOf(shards), response.getSetting("janusgraph", "index.number_of_shards"));

        idx.close();
        n.close();
        esr.stop();
    }

    private void simpleWriteAndQuery(IndexProvider idx) throws BackendException, InterruptedException {

        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "jvmlocal_test_store";
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest.getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures()));

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
