package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SolrTitanIndexTest extends TitanIndexTest {

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        SolrRunner.start();
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
    }


    protected SolrTitanIndexTest() {
        super(true, true, true);
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Test
    public void testRawQueries() {
        clopen(option(SolrIndex.DYNAMIC_FIELDS,TitanIndexTest.INDEX),false);
        super.testRawQueries();
    }

}
