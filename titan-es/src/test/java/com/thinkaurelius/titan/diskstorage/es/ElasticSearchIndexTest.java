package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import java.io.File;

import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CONF_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ElasticSearchIndexTest extends IndexProviderTest {

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new ElasticSearchIndex(getLocalESTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    public static final Configuration getLocalESTestConfig() {
        final String index = "es";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(LOCAL_MODE, true, index);
        config.set(CLIENT_ONLY, false, index);
        config.set(TTL_INTERVAL, "5s", index);
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), index);
        return config.restrictTo(index);
    }


    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_REGEX));
        assertFalse(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.REGEX));
        assertFalse(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Cmp.NOT_EQUAL));
    }

    @Test
    public void testConfiguration() throws BackendException {
        // Test that local-mode has precedence over hostname
        final String index = "es";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(LOCAL_MODE, true, index);
        config.set(CLIENT_ONLY, true, index);
        config.set(INDEX_HOSTS, new String[] { "10.0.0.1" }, index);
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), index);
        Configuration indexConfig = config.restrictTo(index);

        IndexProvider idx = new ElasticSearchIndex(indexConfig); // Shouldn't throw exception
        idx.close();

        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(LOCAL_MODE, false, index);
        config.set(CLIENT_ONLY, true, index);
        config.set(INDEX_HOSTS, new String[] { "10.0.0.1" }, index);
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), index);
        indexConfig = config.restrictTo(index);

        RuntimeException expectedException = null;
        try {
            idx = new ElasticSearchIndex(indexConfig); // Should try 10.0.0.1 and throw exception
            idx.close();
        } catch (RuntimeException re) {
            expectedException = re;
        }
        assertNotNull(expectedException);
    }

    @Test
    public void testConfigurationFile() throws BackendException {
        final String index = "es";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(LOCAL_MODE, true, index);
        config.set(CLIENT_ONLY, true, index);
        config.set(INDEX_CONF_FILE, Joiner.on(File.separator).join("target", "test-classes", "es_nodename_foo.yml"), index);
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), index);
        Configuration indexConfig = config.restrictTo(index);

        ElasticSearchIndex idx = new ElasticSearchIndex(indexConfig); // Shouldn't throw exception
        idx.close();

        assertEquals("foo", idx.getNode().settings().get("node.name"));

        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(LOCAL_MODE, true, index);
        config.set(CLIENT_ONLY, true, index);
        config.set(INDEX_CONF_FILE, Joiner.on(File.separator).join("target", "test-classes", "es_nodename_bar.yml"), index);
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), index);
        indexConfig = config.restrictTo(index);

        idx = new ElasticSearchIndex(indexConfig); // Shouldn't throw exception
        idx.close();

        assertEquals("bar", idx.getNode().settings().get("node.name"));
    }
}
