package org.janusgraph.diskstorage.solr;

import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SolrJanusGraphIndexTest extends JanusGraphIndexTest {

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        SolrRunner.start();
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
    }


    protected SolrJanusGraphIndexTest() {
        super(true, true, true);
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    protected boolean supportsCollections() {
        return false;
    }

    @Test
    public void testRawQueries() {
        clopen(option(SolrIndex.DYNAMIC_FIELDS,JanusGraphIndexTest.INDEX),false);
        super.testRawQueries();
    }

}
