package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrIndexTest extends IndexProviderTest {

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        SolrRunner.start();
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
    }

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new SolrIndex(getLocalSolrTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    private Configuration getLocalSolrTestConfig() {
        final String index = "solr";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();

        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), index);

        return config.restrictTo(index);
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class)));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING))));
        assertFalse(index.supports(of(String.class, new Parameter("mapping",Mapping.TEXTSTRING))));

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
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.DEFAULT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_REGEX));
        assertFalse(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXTSTRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.TEXT)), Text.CONTAINS));
        assertFalse(index.supports(of(String.class, new Parameter("mapping", Mapping.DEFAULT)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, new Parameter("mapping",Mapping.STRING)), Cmp.NOT_EQUAL));
        assertFalse(index.supports(of(String.class, new Parameter("mapping",Mapping.TEXTSTRING)), Cmp.NOT_EQUAL));

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

}
