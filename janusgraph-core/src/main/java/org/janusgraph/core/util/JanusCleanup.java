package org.janusgraph.core.util;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraph;

import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Utility class containing methods that simplify Janus clean-up processes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusCleanup {

    /**
     * Clears out the entire graph. This will delete ALL of the data stored in this graph and the data will NOT be
     * recoverable. This method is intended only for development and testing use.
     *
     * @param graph
     * @throws IllegalArgumentException if the graph has not been shut down
     * @throws org.janusgraph.core.JanusException if clearing the storage is unsuccessful
     */
    public static final void clear(JanusGraph graph) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph instanceof StandardJanusGraph,"Invalid graph instance detected: %s",graph.getClass());
        StandardJanusGraph g = (StandardJanusGraph)graph;
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
        }, Duration.ofSeconds(20));
    }


}
