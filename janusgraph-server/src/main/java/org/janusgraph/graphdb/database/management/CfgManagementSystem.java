// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.database.management;

import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.EnumSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class CfgManagementSystem {
    private final StandardJanusGraph graph;
    private final CfgManagementLogger managementLogger;

    public CfgManagementSystem(StandardJanusGraph graph) {
        this.graph = graph;
        this.managementLogger = (CfgManagementLogger) graph.getManagementLogger();
    }

    /**
     * Upon the open managementsystem's commit, this graph will be asynchronously evicted from the cache on all JanusGraph nodes in your
     * cluster, once there are no open transactions on this graph on each respective JanusGraph node
     * and assuming each node is correctly configured to use the {@link org.janusgraph.graphdb.management.JanusGraphManager}.
     */
    public void evictGraphFromCache() {
        final JanusGraphManagement mgmt = graph.openManagement();
        final Set<String> updatedInstances = ((ManagementSystem) mgmt).getOpenInstancesInternal();
        List<Callable<Boolean>> triggers = Collections.singletonList(new GraphCacheEvictionCompleteTrigger(this.graph.getGraphName()));
        mgmt.commit();
        managementLogger.sendCacheEviction(new HashSet<>(), true, triggers, updatedInstances);
    }

    private static class GraphCacheEvictionCompleteTrigger implements Callable<Boolean> {
        private static final Logger log = LoggerFactory.getLogger(GraphCacheEvictionCompleteTrigger.class);
        private final String graphName;

        private GraphCacheEvictionCompleteTrigger(String graphName) {
            this.graphName = graphName;
        }

        @Override
        public Boolean call() {
            log.info("Graph {} has been removed from the graph cache on every JanusGraph node in the cluster.", graphName);
            return true;
        }
    }
}
