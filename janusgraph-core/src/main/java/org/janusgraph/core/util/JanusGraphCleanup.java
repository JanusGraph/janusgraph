// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core.util;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraph;

import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Utility class containing methods that simplify JanusGraph clean-up processes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphCleanup {

    /**
     * Clears out the entire graph. This will delete ALL of the data stored in this graph and the data will NOT be
     * recoverable. This method is intended only for development and testing use.
     *
     * @param graph
     * @throws IllegalArgumentException if the graph has not been shut down
     * @throws org.janusgraph.core.JanusGraphException if clearing the storage is unsuccessful
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
