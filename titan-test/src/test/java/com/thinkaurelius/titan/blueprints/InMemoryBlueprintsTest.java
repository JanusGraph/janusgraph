package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.QueryTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
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
