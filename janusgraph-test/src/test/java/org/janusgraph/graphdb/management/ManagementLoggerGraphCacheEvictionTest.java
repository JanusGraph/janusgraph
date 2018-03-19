// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.graphdb.management;

import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class ManagementLoggerGraphCacheEvictionTest {

    @After
    public void cleanUp() {
        JanusGraphManager.shutdownJanusGraphManager();
    }

    @Test
    public void shouldNotBeAbleToEvictGraphWhenJanusGraphManagerIsNull() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        final MapConfiguration config = new MapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)));
        final ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        mgmt.evictGraphFromCache();
        mgmt.commit();

        assertNull(JanusGraphManager.getInstance());
    }

    @Test
    public void graphShouldBeRemovedFromCache() throws InterruptedException {
        final JanusGraphManager jgm = new JanusGraphManager(new Settings());
        assertNotNull(jgm);
        assertNotNull(JanusGraphManager.getInstance());
        assertNull(jgm.getGraph("graph1"));

        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        final MapConfiguration config = new MapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)));
        jgm.putGraph("graph1", graph);
        assertEquals("graph1", ((StandardJanusGraph) JanusGraphManager.getInstance().getGraph("graph1")).getGraphName());

        final ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        mgmt.evictGraphFromCache();
        mgmt.commit();

        // wait for log to be asynchronously pulled
        Thread.sleep(10000);

        assertNull(jgm.getGraph("graph1"));
    }
}
