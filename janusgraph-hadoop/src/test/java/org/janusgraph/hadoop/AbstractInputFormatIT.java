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

package org.janusgraph.hadoop;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractInputFormatIT extends JanusGraphBaseTest {

    @Test
    public void testReadGraphOfTheGods() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(12L, (long) t.V().count().next());
    }

    @Test
    public void testReadWideVertexWithManyProperties() throws Exception {
        int numProps = 1 << 16;

        long numV  = 1;
        mgmt.makePropertyKey("p").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.commit();
        finishSchema();

        for (int j = 0; j < numV; j++) {
            Vertex v = graph.addVertex();
            for (int i = 0; i < numProps; i++) {
                v.property("p", i);
                if (i % 8 == 0) {
                    graph.tx().commit();
                }
            }
        }
        graph.tx().commit();

        assertEquals(numV, (long) graph.traversal().V().count().next());
        Map<Object, Object> propertiesOnVertex = graph.traversal().V().valueMap().next();
        List<?> valuesOnP = (List)propertiesOnVertex.values().iterator().next();
        assertEquals(numProps, valuesOnP.size());
        for (int i = 0; i < numProps; i++) {
            assertEquals(Integer.toString(i), valuesOnP.get(i).toString());
        }
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(numV, (long) t.V().count().next());
        propertiesOnVertex = t.V().valueMap().next();
        final Set<?> observedValuesOnP = Collections.unmodifiableSet(new HashSet<>((List)propertiesOnVertex.values().iterator().next()));
        assertEquals(numProps, observedValuesOnP.size());
        // order may not be preserved in multi-value properties
        assertEquals(Collections.unmodifiableSet(new HashSet<>(valuesOnP)), observedValuesOnP, "Unexpected values");
    }

    @Test
    public void testReadSelfEdge() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());

        // Add a self-loop on sky with edge label "lives"; it's nonsense, but at least it needs no schema changes
        JanusGraphVertex sky = graph.query().has("name", "sky").vertices().iterator().next();
        assertNotNull(sky);
        assertEquals("sky", sky.value("name"));
        assertEquals(1L, sky.query().direction(Direction.IN).edgeCount());
        assertEquals(0L, sky.query().direction(Direction.OUT).edgeCount());
        assertEquals(1L, sky.query().direction(Direction.BOTH).edgeCount());
        sky.addEdge("lives", sky, "reason", "testReadSelfEdge");
        assertEquals(2L, sky.query().direction(Direction.IN).edgeCount());
        assertEquals(1L, sky.query().direction(Direction.OUT).edgeCount());
        assertEquals(3L, sky.query().direction(Direction.BOTH).edgeCount());
        graph.tx().commit();

        // Read the new edge using the inputformat
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        Iterator<Object> edgeIdIterator = t.V().has("name", "sky").bothE().id();
        assertNotNull(edgeIdIterator);
        assertTrue(edgeIdIterator.hasNext());
        Set<Object> edges = new HashSet<>();
        edgeIdIterator.forEachRemaining(edges::add);
        assertEquals(2, edges.size());
    }

    @Test
    public void testReadMultipleSelfEdges() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());

        // Similarly to testReadSelfEdge(), add multiple self-loop edges on sky with edge label "lives"
        JanusGraphVertex sky = graph.query().has("name", "sky").vertices().iterator().next();
        assertNotNull(sky);
        assertEquals("sky", sky.value("name"));
        assertEquals(1L, sky.query().direction(Direction.IN).edgeCount());
        assertEquals(0L, sky.query().direction(Direction.OUT).edgeCount());
        assertEquals(1L, sky.query().direction(Direction.BOTH).edgeCount());
        sky.addEdge("lives", sky, "reason", "testReadMultipleSelfEdges1");
        sky.addEdge("lives", sky, "reason", "testReadMultipleSelfEdges2");
        sky.addEdge("lives", sky, "reason", "testReadMultipleSelfEdges3");
        assertEquals(4L, sky.query().direction(Direction.IN).edgeCount());
        assertEquals(3L, sky.query().direction(Direction.OUT).edgeCount());
        assertEquals(7L, sky.query().direction(Direction.BOTH).edgeCount());
        graph.tx().commit();

        // Read all the new edges using the inputformat
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        Iterator<Object> edgeIdIterator = t.V().has("name", "sky").bothE().id();
        assertNotNull(edgeIdIterator);
        assertTrue(edgeIdIterator.hasNext());
        Set<Object> edges = new HashSet<>();
        edgeIdIterator.forEachRemaining(edges::add);
        assertEquals(4, edges.size());
    }

    @Test
    public void testGeoshapeGetValues() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);

        // Read geoshape using the input format
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        Iterator<Object> geoIterator = t.E().values("place");
        assertNotNull(geoIterator);
        assertTrue(geoIterator.hasNext());
        Set<Object> geoShapes = new HashSet<>();
        geoIterator.forEachRemaining(geoShapes::add);
        assertEquals(3, geoShapes.size());
    }

    @Test
    public void testReadGraphOfTheGodsWithEdgeFiltering() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(17L, (long) graph.traversal().E().count().next());

        // Read graph filtering out "battled" edges.
        Graph g = getGraph();
        Computer computer = Computer.compute(SparkGraphComputer.class)
            .edges(__.bothE().hasLabel(P.neq("battled")));
        GraphTraversalSource t = g.traversal().withComputer(computer);
        assertEquals(14L, (long) t.E().count().next());
    }

    @Test
    public void testGraphWithIsolatedVertices() throws Exception {
        String key = "vertexKey";

        graph.addVertex(key);
        graph.tx().commit();

        // Read graph using the inputformat.
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(1L, (long) t.V().count().next());
    }

    @Test
    public void testSchemaVerticesAreSkipped() throws Exception {
        mgmt.makePropertyKey("p").dataType(Integer.class).make();
        mgmt.makeVertexLabel("v").make();
        mgmt.makeEdgeLabel("e").make();
        finishSchema();

        // Read graph using the inputformat.
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(0L, (long) t.V().count().next());
    }

    @Test
    public void testReadWithMetaProperties() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        GraphTraversalSource t = graph.traversal();

        assertEquals(0L, (long) t.V().has("name", "sky").properties("property").count().next());

        mgmt.makePropertyKey("prop").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        mgmt.makePropertyKey("meta_property").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        mgmt.commit();
        finishSchema();

        t.V().has("name", "sky")
            .property("prop", "value")
            .iterate();
        graph.tx().commit();
        assertEquals(1L, (long) t.V().has("name", "sky").properties("prop").count().next());
        assertEquals(0L, (long) t.V().has("name", "sky").properties("prop")
            .properties("meta_property").count().next());

        t.V()
            .has("name", "sky")
            .properties("prop")
            .property("meta_property", "meta_value")
            .iterate();
        graph.tx().commit();
        assertEquals(1L, (long) t.V().has("name", "sky").properties("prop")
            .properties("meta_property").count().next());

        Graph g = getGraph();
        t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(1L, (long) t.V().has("name", "sky").properties("prop").count().next());
        assertEquals(1L, (long) t.V().has("name", "sky").properties("prop")
            .properties("meta_property").count().next());
    }

    protected abstract Graph getGraph() throws IOException, ConfigurationException;
}
