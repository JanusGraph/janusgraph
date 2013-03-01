package com.thinkaurelius.titan.diskstorage.lucene;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Txt;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.diskstorage.lucene.LuceneIndex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
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

        assertTrue(index.supports(String.class, Txt.CONTAINS));
        assertTrue(index.supports(Double.class, Cmp.EQUAL));
        assertTrue(index.supports(Double.class, Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(Double.class, Cmp.LESS_THAN));
        assertTrue(index.supports(Long.class, Cmp.INTERVAL));
        assertTrue(index.supports(Geoshape.class, Geo.WITHIN));

        assertFalse(index.supports(String.class, Txt.PREFIX));
        assertFalse(index.supports(Double.class, Geo.INTERSECT));
        assertFalse(index.supports(Long.class, Txt.CONTAINS));
        assertFalse(index.supports(Geoshape.class, Geo.DISJOINT));
    }

}
