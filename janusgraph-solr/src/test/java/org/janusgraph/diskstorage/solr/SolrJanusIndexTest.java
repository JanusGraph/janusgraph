package org.janusgraph.diskstorage.solr;

import org.janusgraph.graphdb.JanusIndexTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SolrJanusIndexTest extends JanusIndexTest {

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        SolrRunner.start();
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
    }


    protected SolrJanusIndexTest() {
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
        clopen(option(SolrIndex.DYNAMIC_FIELDS,JanusIndexTest.INDEX),false);
        super.testRawQueries();
    }

}
