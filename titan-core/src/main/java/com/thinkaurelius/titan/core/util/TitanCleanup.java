package com.thinkaurelius.titan.core.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import java.util.concurrent.Callable;

/**
 * Utility class containing methods that simplify Titan clean-up processes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanCleanup {

    /**
     * Clears out the entire graph. This will delete ALL of the data stored in this graph and the data will NOT be
     * recoverable. This method is intended only for development and testing use.
     *
     * @param graph
     * @throws IllegalArgumentException if the graph has not been shut down
     * @throws com.thinkaurelius.titan.core.TitanException if clearing the storage is unsuccessful
     */
    public static final void clear(TitanGraph graph) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph instanceof StandardTitanGraph,"Invalid graph instance detected: %s",graph.getClass());
        StandardTitanGraph g = (StandardTitanGraph)graph;
        Preconditions.checkArgument(!g.isOpen(),"Graph needs to be shut down before it can be cleared.");
        final GraphDatabaseConfiguration config = g.getConfiguration();
        BackendOperation.execute(new Callable<Boolean>(){
            @Override
            public Boolean call() throws Exception {
                config.getBackend().clearStorage();
                return true;
            }
            @Override
            public String toString() { return "ClearBackend"; }
        },config.getWriteAttempts(),config.getStorageWaittime());
    }


}
