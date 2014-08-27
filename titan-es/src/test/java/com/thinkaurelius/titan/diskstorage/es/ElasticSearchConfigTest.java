package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CONF_FILE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

/**
 * Test behavior Titan ConfigOptions governing ES client setup.
 *
 * {@link ElasticSearchIndexTest#testConfiguration()} exercises legacy
 * config options using an embedded JVM-local-transport ES instance.  By contrast,
 * this class exercises the new {@link ElasticSearchIndex#INTERFACE} configuration
 * mechanism and uses a network-capable embedded ES instance.
 */
public class ElasticSearchConfigTest {

    private static final String INDEX_NAME = "escfg";

    @Test
    public void testTransportClient() throws BackendException {
        ElasticsearchRunner esr = new ElasticsearchRunner();
        esr.start();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.TRANSPORT_CLIENT, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        idx.close();

        config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.TRANSPORT_CLIENT, INDEX_NAME);
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
    public void testNetworkNodeUsingExt() throws BackendException {
        ElasticsearchRunner esr = new ElasticsearchRunner();
        esr.start();
        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.data", "false");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.node.client", "true");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.discovery.zen.ping.multicast.enabled", "false");
        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.discovery.zen.ping.unicast.hosts", "localhost");
        ModifiableConfiguration config =
                new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        idx.close();

        cc.set("index." + INDEX_NAME + ".elasticsearch.ext.discovery.zen.ping.unicast.hosts", "10.11.12.13");
        config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                 cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.NODE, INDEX_NAME);
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
    public void testNetworkNodeUsingYaml() throws BackendException {
        ElasticsearchRunner esr = new ElasticsearchRunner();
        esr.start();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.NODE, INDEX_NAME);
        config.set(INDEX_CONF_FILE,
                Joiner.on(File.separator).join("target", "test-classes", "es_cfg_nodeclient.yml"), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = new ElasticSearchIndex(indexConfig);
        idx.close();

        config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.NODE, INDEX_NAME);
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
        // idx.close();
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);
        esr.stop();
    }
}
