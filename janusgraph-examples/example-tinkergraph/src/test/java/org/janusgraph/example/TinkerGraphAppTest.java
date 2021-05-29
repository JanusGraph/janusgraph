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

package org.janusgraph.example;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TinkerGraphAppTest {
    protected static final String CONF_FILE = "conf/jgex-tinkergraph.properties";

    @Test
    public void createSchema() throws ConfigurationException, IOException {
        final TinkerGraphApp app = new TinkerGraphApp(CONF_FILE);
        final GraphTraversalSource g = app.openGraph();
        app.createSchema();
        final TinkerGraph tinkerGraph = (TinkerGraph) g.getGraph();
        final Set<String> vertexIndexes = tinkerGraph.getIndexedKeys(TinkerVertex.class);
        assertEquals(1, vertexIndexes.size());
        assertEquals("name", vertexIndexes.toArray()[0]);
        final Set<String> edgeIndexes = tinkerGraph.getIndexedKeys(TinkerEdge.class);
        assertTrue(edgeIndexes.isEmpty());
    }
}
