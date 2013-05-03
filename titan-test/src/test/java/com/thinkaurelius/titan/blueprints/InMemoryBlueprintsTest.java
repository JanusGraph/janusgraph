package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class InMemoryBlueprintsTest extends TitanBlueprintsTest {

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this), ImmutableSet.of("testStringRepresentation","testDataTypeValidationOnProperties","testGraphDataPersists"));
        BaseTest.printTestPerformance("GraphTestSuite", this.stopWatch());
    }



    ///=========================== DEFAULT ===========

    @Override
    public void cleanUp() throws StorageException {

    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public void startUp() {

    }

    @Override
    public void shutDown() {

    }

    @Override
    public Graph generateGraph() {
        TitanGraph graph = StorageSetup.getInMemoryGraph();
        return graph;
    }

    @Override
    public Graph generateGraph(String graphDirectoryName) {
        throw new UnsupportedOperationException();
    }


}
