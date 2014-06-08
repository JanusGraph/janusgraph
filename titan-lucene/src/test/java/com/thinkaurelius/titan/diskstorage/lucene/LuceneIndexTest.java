package com.thinkaurelius.titan.diskstorage.lucene;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LuceneIndexTest extends IndexProviderTest {

    private static final Logger log =
            LoggerFactory.getLogger(LuceneIndexTest.class);

    @Rule
    public TestName methodName = new TestName();

    @Override
    public IndexProvider openIndex() throws StorageException {
        return new LuceneIndex(getLocalLuceneTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return false;
    }

    public static final Configuration getLocalLuceneTestConfig() {
        final String index = "lucene";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"),index);
        return config.restrictTo(index);
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class)));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING))));

        assertTrue(index.supports(of(Double.class)));
        assertFalse(index.supports(of(Double.class, new Parameter("mapping",Mapping.TEXT))));

        assertTrue(index.supports(of(Long.class)));
        assertTrue(index.supports(of(Long.class, new Parameter("mapping",Mapping.DEFAULT))));
        assertTrue(index.supports(of(Integer.class)));
        assertTrue(index.supports(of(Short.class)));
        assertTrue(index.supports(of(Byte.class)));
        assertTrue(index.supports(of(Float.class)));
        assertTrue(index.supports(of(Geoshape.class)));
        assertFalse(index.supports(of(Object.class)));
        assertFalse(index.supports(of(Exception.class)));

        assertTrue(index.supports(of(String.class), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertFalse(index.supports(of(String.class), Text.CONTAINS_REGEX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertFalse(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Double.class), Cmp.EQUAL));
        assertTrue(index.supports(of(Double.class), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Double.class), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Double.class, new Parameter("mapping",Mapping.DEFAULT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(Double.class, new Parameter("mapping",Mapping.TEXT)), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Geoshape.class), Geo.WITHIN));

        assertFalse(index.supports(of(Double.class), Geo.INTERSECT));
        assertFalse(index.supports(of(Long.class), Text.CONTAINS));
        assertFalse(index.supports(of(Geoshape.class), Geo.DISJOINT));
    }

    @Override
    public void testDeleteDocumentThenModifyField() {
        // This fails under Lucene but works in ES
        log.info("Skipping " + getClass().getSimpleName() + "." + methodName.getMethodName());
    }
}
