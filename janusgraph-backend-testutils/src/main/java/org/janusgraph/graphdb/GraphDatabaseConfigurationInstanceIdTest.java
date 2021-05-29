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

package org.janusgraph.graphdb;

import org.apache.commons.configuration2.MapConfiguration;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REPLACE_INSTANCE_IF_EXISTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GraphDatabaseConfigurationInstanceIdTest {

    @Test
    public void graphShouldOpenWithSameInstanceId() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(UNIQUE_INSTANCE_ID.toStringWithoutRoot(), "not-unique");
        map.put(REPLACE_INSTANCE_IF_EXISTS.toStringWithoutRoot(), true);
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        final StandardJanusGraph graph1 = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));

        assertEquals(graph1.openManagement().getOpenInstances().size(), 1);
        assertEquals(graph1.openManagement().getOpenInstances().toArray()[0], "not-unique");

        final StandardJanusGraph graph2 = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));

        assertEquals(graph1.openManagement().getOpenInstances().size(), 1);
        assertEquals(graph1.openManagement().getOpenInstances().toArray()[0], "not-unique");
        assertEquals(graph2.openManagement().getOpenInstances().size(), 1);
        assertEquals(graph2.openManagement().getOpenInstances().toArray()[0], "not-unique");
        graph1.close();
        graph2.close();
    }

    @Test
    public void graphShouldNotOpenWithSameInstanceId() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(UNIQUE_INSTANCE_ID.toStringWithoutRoot(), "not-unique");
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        final StandardJanusGraph graph1 = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));

        assertEquals(graph1.openManagement().getOpenInstances().size(), 1);
        assertEquals(graph1.openManagement().getOpenInstances().toArray()[0], "not-unique");
        JanusGraphException janusGraphException = assertThrows(JanusGraphException.class, () -> {
            final StandardJanusGraph graph2 = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph1.close();
        });
        assertEquals("A JanusGraph graph with the same instance id [not-unique] is already open. Might required forced shutdown.", janusGraphException.getMessage());
    }

    @Test
    public void instanceIdShouldEqualHostname() throws UnknownHostException {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
        assertEquals(graph.openManagement().getOpenInstances().size(), 1);
        assertEquals(graph.openManagement().getOpenInstances().toArray()[0], Inet4Address.getLocalHost().getHostName());
				graph.close();
    }

    @Test
    public void instanceIdShouldEqualHostnamePlusSuffix() throws UnknownHostException {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(UNIQUE_INSTANCE_ID_SUFFIX.toStringWithoutRoot(), 1);
				final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
        assertEquals(graph.openManagement().getOpenInstances().size(), 1);
        assertEquals(graph.openManagement().getOpenInstances().toArray()[0], Inet4Address.getLocalHost().getHostName() + "1");
				graph.close();
    }
}
