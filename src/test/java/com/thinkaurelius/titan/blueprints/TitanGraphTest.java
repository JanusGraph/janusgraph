package com.thinkaurelius.titan.blueprints;

import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import org.junit.Test;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanGraphTest extends GraphTest {


    @Override
    public Graph getGraphInstance() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
    
    @Test
    public void testNaive() {
        assertTrue(true);
    }
}
