package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.BackendException;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanBlueprintsTest {

    protected Map<String,TitanGraph> openGraphs;


    public void cleanUp() throws BackendException {

    }

    public boolean supportsMultipleGraphs() {
        return false;
    }

    public void beforeSuite() {

    }

    public void afterSuite() {

    }

    public void extraCleanUp(String uid) throws BackendException {

    }

    public void beforeOpeningGraph(String uid) {

    }

    public abstract TitanGraph openGraph(String uid) throws BackendException;

    public TitanGraph generateGraph() {
        TitanGraph graph = StorageSetup.getInMemoryGraph();
        return graph;
    }

    public TitanGraph generateGraph(String graphDirectoryName) {
        throw new UnsupportedOperationException();
    }

}
