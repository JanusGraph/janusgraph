package com.thinkaurelius.titan.diskstorage.lucene;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LuceneIndexTest extends IndexProviderTest {

    @Override
    public IndexProvider openIndex() throws StorageException {
        return new LuceneIndex(getLocalLuceneTestConfig());
    }

    public static final Configuration getLocalLuceneTestConfig() {
        Configuration config = new BaseConfiguration();
        config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("lucene"));
        return config;
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(String.class));
        assertTrue(index.supports(Double.class));
        assertTrue(index.supports(Long.class));
        assertTrue(index.supports(Integer.class));
        assertTrue(index.supports(Geoshape.class));
        assertFalse(index.supports(Object.class));
        assertFalse(index.supports(Exception.class));

        assertTrue(index.supports(String.class, Text.CONTAINS));
        assertTrue(index.supports(String.class, Text.PREFIX));
        assertFalse(index.supports(String.class, Text.REGEX));
        assertTrue(index.supports(Double.class, Cmp.EQUAL));
        assertTrue(index.supports(Double.class, Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(Double.class, Cmp.LESS_THAN));
        assertTrue(index.supports(Geoshape.class, Geo.WITHIN));

        assertFalse(index.supports(Double.class, Geo.INTERSECT));
        assertFalse(index.supports(Long.class, Text.CONTAINS));
        assertFalse(index.supports(Geoshape.class, Geo.DISJOINT));
    }

}
