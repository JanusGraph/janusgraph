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

import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This test suite contains the same tests as in HBaseInputFormatIT and AbstractInputFormatIT, but
 * takes HBase snapshots of the graph table during the tests. The snapshots are used by
 * HBaseSnapshotInputFormat.
 */
public class HBaseSnapshotInputFormatIT extends AbstractInputFormatIT {

    // Used by this test only. Need to be consistent with hbase-read-snapshot.properties
    private final String table = "janusgraph";
    private final String snapshotName = "janusgraph-snapshot";

    @BeforeAll
    public static void startHBase() throws IOException, BackendException {
        HBaseStorageSetup.startHBase();
    }

    @AfterAll
    public static void stopHBase() {
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        HBaseStorageSetup.deleteSnapshot(snapshotName);
    }

    @Test
    @Override
    public void testReadGraphOfTheGods() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());
        // Take a snapshot of the graph table
        HBaseStorageSetup.createSnapshot(snapshotName, table);

        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(12L, (long) t.V().count().next());
    }

    @Test
    @Override
    public void testReadWideVertexWithManyProperties() throws Exception {
        int numProps = 1 << 16;

        long numV = 1;
        mgmt.makePropertyKey("p").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.commit();
        finishSchema();

        for (int j = 0; j < numV; j++) {
            Vertex v = graph.addVertex();
            for (int i = 0; i < numProps; i++) {
                v.property("p", i);
            }
        }
        graph.tx().commit();

        assertEquals(numV, (long) graph.traversal().V().count().next());
        Map<Object, Object> propertiesOnVertex = graph.traversal().V().valueMap().next();
        List<?> valuesOnP = (List) propertiesOnVertex.values().iterator().next();
        assertEquals(numProps, valuesOnP.size());
        for (int i = 0; i < numProps; i++) {
            assertEquals(Integer.toString(i), valuesOnP.get(i).toString());
        }
        // Take a snapshot of the graph table
        HBaseStorageSetup.createSnapshot(snapshotName, table);

        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(numV, (long) t.V().count().next());
        propertiesOnVertex = t.V().valueMap().next();
        final Set<?> observedValuesOnP = ImmutableSet.copyOf((List) propertiesOnVertex.values().iterator().next());
        assertEquals(numProps, observedValuesOnP.size());
        // order may not be preserved in multi-value properties
        assertEquals(ImmutableSet.copyOf(valuesOnP), observedValuesOnP, "Unexpected values");
    }

    @Test
    @Override
    public void testReadSelfEdge() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());

        // Add a self-loop on sky with edge label "lives"; it's nonsense, but at least it needs no
        // schema changes
        JanusGraphVertex sky =
            graph.query().has("name", "sky").vertices().iterator().next();
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
        // Take a snapshot of the graph table
        HBaseStorageSetup.createSnapshot(snapshotName, table);

        // Read the new edge using the inputformat
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        Iterator<Object> edgeIdIter = t.V().has("name", "sky").bothE().id();
        assertNotNull(edgeIdIter);
        assertTrue(edgeIdIter.hasNext());
        Set<Object> edges = Sets.newHashSet(edgeIdIter);
        assertEquals(2, edges.size());
    }

    @Test
    @Override
    public void testGeoshapeGetValues() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        // Take a snapshot of the graph table
        HBaseStorageSetup.createSnapshot(snapshotName, table);

        // Read geoshape using the inputformat
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        Iterator<Object> geoIter = t.E().values("place");
        assertNotNull(geoIter);
        assertTrue(geoIter.hasNext());
        Set<Object> geos = Sets.newHashSet(geoIter);
        assertEquals(3, geos.size());
    }

    @Test
    @Override
    public void testReadGraphOfTheGodsWithEdgeFiltering() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(17L, (long) graph.traversal().E().count().next());
        // Take a snapshot of the graph table
        HBaseStorageSetup.createSnapshot(snapshotName, table);

        // Read graph filtering out "battled" edges.
        Graph g = getGraph();
        Computer computer = Computer.compute(SparkGraphComputer.class)
            .edges(__.bothE().hasLabel(P.neq("battled")));
        GraphTraversalSource t = g.traversal().withComputer(computer);
        assertEquals(14L, (long) t.E().count().next());
    }

    protected Graph getGraph() throws IOException, ConfigurationException {
        final PropertiesConfiguration config =
                new PropertiesConfiguration("target/test-classes/hbase-read-snapshot.properties");
        Path baseOutDir = Paths.get((String) config.getProperty("gremlin.hadoop.outputLocation"));
        baseOutDir.toFile().mkdirs();
        String outDir = Files.createTempDirectory(baseOutDir, null).toAbsolutePath().toString();
        config.setProperty("gremlin.hadoop.outputLocation", outDir);
        // Set the hbase.rootdir property. This is needed by HBaseSnapshotInputFormat.
        config.setProperty("janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.rootdir",
            HBaseStorageSetup.getHBaseRootdir());
        return GraphFactory.open(config);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return HBaseStorageSetup.getHBaseGraphConfiguration();
    }
}
