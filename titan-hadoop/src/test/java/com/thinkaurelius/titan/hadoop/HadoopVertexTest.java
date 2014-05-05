package com.thinkaurelius.titan.hadoop;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopVertexTest extends BaseTest {

    public void testRawComparator() throws IOException {
        HadoopVertex vertex1 = new HadoopVertex(EmptyConfiguration.immutable(), 10);
        HadoopVertex vertex2 = new HadoopVertex(EmptyConfiguration.immutable(), 11);

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        vertex2.write(new DataOutputStream(bytes2));

        assertEquals(-1, new HadoopSerializer.Comparator().compare(bytes1.toByteArray(), 0, bytes1.size(), bytes2.toByteArray(), 0, bytes2.size()));
        assertEquals(1, new HadoopSerializer.Comparator().compare(bytes2.toByteArray(), 0, bytes2.size(), bytes1.toByteArray(), 0, bytes1.size()));
        assertEquals(0, new HadoopSerializer.Comparator().compare(bytes1.toByteArray(), 0, bytes1.size(), bytes1.toByteArray(), 0, bytes1.size()));

        System.out.println("Vertex with 0 properties has a byte size of: " + bytes1.toByteArray().length);
    }

    public void testSimpleSerialization() throws IOException {

        HadoopVertex vertex1 = new HadoopVertex(EmptyConfiguration.immutable(), 10l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        vertex1.write(out);

        // schema length is 1 byte
        // id length is 1 byte (variable long)
        // paths size 1 byte (variable int)
        // properties size 1 byte (variable int)
        // out edge types size 1 byte (variable int)
        // in edge types size 1 byte (variable int)
        assertEquals(7, bytes.toByteArray().length);
        HadoopVertex vertex2 = new HadoopVertex(new EmptyConfiguration(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        System.out.println("Vertex with 0 properties has a byte size of: " + bytes.toByteArray().length);

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertFalse(vertex1.hasPaths());
        assertFalse(vertex2.hasPaths());
        assertEquals(vertex1.pathCount(), 0);
        assertEquals(vertex2.pathCount(), 0);
        assertFalse(vertex1.trackPaths);
        assertFalse(vertex2.trackPaths);
        assertFalse(vertex2.getEdges(Direction.OUT).iterator().hasNext());
        assertFalse(vertex2.getEdges(Direction.IN).iterator().hasNext());
        assertFalse(vertex2.getEdges(Direction.BOTH).iterator().hasNext());
        assertEquals(vertex2.getPropertyKeys().size(), 0);

    }

    public void testVertexSerialization() throws IOException {

        HadoopVertex vertex1 = new HadoopVertex(EmptyConfiguration.immutable(), 10);
        vertex1.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex1.getIdAsLong(), 2, "knows"));
        vertex1.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex1.getIdAsLong(), 3, "knows"));
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);
        vertex1.setProperty("longitude", 10.01d);
        vertex1.setProperty("latitude", 11.399f);
        vertex1.setProperty("size", 10l);
        vertex1.setProperty("boolean", true);
        vertex1.addProperty("home", "New Mexico");
        vertex1.addProperty("home", "California");
        assertEquals(vertex1.getPropertyKeys().size(), 7);
        assertEquals(vertex1.getProperty("name"), "marko");
        assertEquals(vertex1.getProperty("age"), 32);
        assertEquals(vertex1.getProperty("longitude"), 10.01d);
        assertEquals(vertex1.getProperty("latitude"), 11.399f);
        assertEquals(vertex1.getProperty("size"), 10l);
        assertTrue((Boolean) vertex1.getProperty("boolean"));
        assertEquals(2, Iterables.size(vertex1.getProperties("home")));
        for (Object p : vertex1.getProperties("home")) {
            assertTrue(ImmutableSet.of("New Mexico", "California").contains(p));
        }
        assertEquals(8, Iterables.size(vertex1.getProperties()));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        vertex1.write(new DataOutputStream(bytes));
        HadoopVertex vertex2 = new HadoopVertex(new EmptyConfiguration(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        System.out.println("Vertex with 6 properties and 2 outgoing edges has a byte size of: " + bytes.toByteArray().length);

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertEquals(vertex1.getPropertyKeys().size(), 7);
        assertEquals(vertex1.getProperty("name"), "marko");
        assertEquals(vertex1.getProperty("age"), 32);
        assertEquals(vertex1.getProperty("longitude"), 10.01d);
        assertEquals(vertex1.getProperty("latitude"), 11.399f);
        assertEquals(vertex1.getProperty("size"), 10l);
        assertTrue((Boolean) vertex1.getProperty("boolean"));
        assertEquals(2, Iterables.size(vertex1.getProperties("home")));
        for (Object p : vertex1.getProperties("home")) {
            assertTrue(ImmutableSet.of("New Mexico", "California").contains(p));
        }
        assertEquals(8, Iterables.size(vertex1.getProperties()));

        Iterator<Edge> edges = vertex2.getEdges(Direction.OUT).iterator();
        assertTrue(edges.hasNext());
        assertEquals(edges.next().getLabel(), "knows");
        assertTrue(edges.hasNext());
        assertEquals(edges.next().getLabel(), "knows");
        assertFalse(edges.hasNext());

    }

    public void testVertexSerializationWithPaths() throws IOException {
        Configuration configuration = new EmptyConfiguration();
        configuration.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);

        HadoopVertex vertex1 = new HadoopVertex(EmptyConfiguration.immutable(), 10);
        vertex1.setConf(configuration);
        vertex1.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex1.getIdAsLong(), 2, "knows"));
        vertex1.addEdge(IN, new HadoopEdge(EmptyConfiguration.immutable(), 3, vertex1.getIdAsLong(), "knows"));
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);
        vertex1.setProperty("longitude", 10.01d);
        vertex1.setProperty("latitude", 11.399f);
        vertex1.setProperty("size", 10l);
        assertEquals(vertex1.getPropertyKeys().size(), 5);
        assertEquals(vertex1.getProperty("name"), "marko");
        assertEquals(vertex1.getProperty("age"), 32);
        assertEquals(vertex1.getProperty("longitude"), 10.01d);
        assertEquals(vertex1.getProperty("latitude"), 11.399f);
        assertEquals(vertex1.getProperty("size"), 10l);
        vertex1.addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(10l), new HadoopVertex.MicroVertex(1l)), false);
        vertex1.addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(10l), new HadoopVertex.MicroVertex(2l)), false);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        vertex1.write(new DataOutputStream(bytes));

        HadoopVertex vertex2 = new HadoopVertex(configuration, new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        System.out.println("Vertex with 4 properties and 2 paths has a byte size of: " + bytes.toByteArray().length);

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertEquals(vertex2.getPropertyKeys().size(), 5);
        assertEquals(vertex2.getProperty("name"), "marko");
        assertEquals(vertex2.getProperty("age"), 32);
        assertEquals(vertex2.getProperty("longitude"), 10.01d);
        assertEquals(vertex2.getProperty("latitude"), 11.399f);
        assertEquals(vertex1.getProperty("size"), 10l);
        assertEquals(vertex2.pathCount(), 2);
        assertTrue(vertex2.hasPaths());
        for (List<HadoopPathElement.MicroElement> path : vertex2.getPaths()) {
            assertEquals(path.get(0).getId(), 10l);
            assertTrue(path.get(1).getId() == 1l || path.get(1).getId() == 2l);
            assertEquals(path.size(), 2);
        }

        Iterator<Edge> edges = vertex2.getEdges(Direction.OUT).iterator();
        assertTrue(edges.hasNext());
        Edge edge = edges.next();
        assertEquals(edge.getLabel(), "knows");
        assertEquals(edge.getVertex(Direction.IN).getId(), 2l);
        assertEquals(edge.getVertex(Direction.OUT).getId(), 10l);
        assertFalse(edges.hasNext());

        edges = vertex2.getEdges(Direction.IN).iterator();
        assertTrue(edges.hasNext());
        edge = edges.next();
        assertEquals(edge.getLabel(), "knows");
        assertEquals(edge.getVertex(Direction.IN).getId(), 10l);
        assertEquals(edge.getVertex(Direction.OUT).getId(), 3l);
        assertEquals(edge.getLabel(), "knows");
        assertFalse(edges.hasNext());

    }

    public void testVertexSerializationNoProperties() throws IOException {
        HadoopVertex vertex1 = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        vertex1.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex1.getIdAsLong(), vertex1.getIdAsLong(), "knows"));

        assertNull(vertex1.getProperty("name"));
        assertNull(vertex1.removeProperty("name"));
        assertEquals(vertex1.getPropertyKeys().size(), 0);
        assertEquals(vertex1.getProperties().size(), 0);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes));
        HadoopVertex vertex2 = new HadoopVertex(EmptyConfiguration.immutable(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        System.out.println("Vertex with 0 properties and 1 outgoing edge has a byte size of: " + bytes.toByteArray().length);

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.getId(), 1l);
        assertNull(vertex1.getProperty("name"));
        assertNull(vertex1.removeProperty("name"));
        assertEquals(vertex1.getPropertyKeys().size(), 0);
        assertEquals(vertex1.getProperties().size(), 0);
        assertEquals(asList(vertex1.getEdges(OUT)).size(), 1);
        assertEquals(asList(vertex1.getEdges(IN)).size(), 0);
        assertEquals(asList(vertex1.getEdges(BOTH)).size(), 1);

        assertEquals(vertex2, vertex1);
        assertEquals(vertex2.getId(), 1l);
        assertNull(vertex2.getProperty("age"));
        assertNull(vertex2.removeProperty("age"));
        assertEquals(vertex2.getPropertyKeys().size(), 0);
        assertEquals(vertex2.getProperties().size(), 0);
        assertEquals(asList(vertex2.getEdges(OUT)).size(), 1);
        assertEquals(asList(vertex2.getEdges(IN)).size(), 0);
        assertEquals(asList(vertex2.getEdges(BOTH)).size(), 1);
    }

    public void testRemovingEdges() {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        vertex.setProperty("name", "marko");
        vertex.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        assertEquals(asList(vertex.getEdges(OUT)).size(), 2);
        vertex.removeEdges(Tokens.Action.DROP, OUT, "knows");
        assertEquals(asList(vertex.getEdges(OUT)).size(), 1);
        assertEquals(vertex.getEdges(OUT).iterator().next().getLabel(), "created");
        assertEquals(vertex.getProperty("name"), "marko");

        vertex = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        vertex.setProperty("name", "marko");
        vertex.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        vertex.addEdge(IN, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(IN, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        assertEquals(asList(vertex.getEdges(OUT)).size(), 2);
        vertex.removeEdges(Tokens.Action.DROP, BOTH, "knows");
        assertEquals(asList(vertex.getEdges(BOTH)).size(), 2);
        assertEquals(vertex.getEdges(OUT).iterator().next().getLabel(), "created");
        assertEquals(vertex.getProperty("name"), "marko");

        vertex = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        vertex.setProperty("name", "marko");
        vertex.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(OUT, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        vertex.addEdge(IN, new HadoopEdge(EmptyConfiguration.immutable(), vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        assertEquals(asList(vertex.getEdges(OUT)).size(), 2);
        vertex.removeEdges(Tokens.Action.KEEP, BOTH, "knows");
        assertEquals(asList(vertex.getEdges(OUT)).size(), 1);
        assertEquals(vertex.getEdges(OUT).iterator().next().getLabel(), "knows");
        assertEquals(vertex.getProperty("name"), "marko");
    }

    public void testGetVerticesAndQuery() throws Exception {
        Map<Long, HadoopVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, new Configuration());
        for (HadoopVertex vertex : graph.values()) {
            if (vertex.getId().equals(1l)) {
                assertFalse(vertex.getVertices(IN).iterator().hasNext());
                assertTrue(vertex.getVertices(OUT).iterator().hasNext());
                assertTrue(vertex.getVertices(BOTH).iterator().hasNext());
                assertEquals(asList(vertex.getVertices(OUT)).size(), 3);
                int id2 = 0;
                int id3 = 0;
                int id4 = 0;
                for (Vertex temp : vertex.getVertices(OUT)) {
                    long id = (Long) temp.getId();
                    if (id == 2l)
                        id2++;
                    else if (id == 3l)
                        id3++;
                    else if (id == 4l)
                        id4++;
                    else
                        assertTrue(false);
                }

                assertEquals(asList(vertex.getVertices(OUT, "created")).size(), 1);
                assertEquals(asList(vertex.getVertices(OUT, "knows")).size(), 2);
                assertEquals(vertex.getVertices(OUT, "created").iterator().next().getId(), 3l);

                assertEquals(id2, 1);
                assertEquals(id3, 1);
                assertEquals(id4, 1);
                assertEquals(asList(vertex.query().has("weight", 0.5).limit(1).vertices()).size(), 1);
                assertEquals(vertex.query().has("weight", 0.5).limit(1).vertices().iterator().next().getId(), 2l);
            }
        }
    }

    public void testGetEdges() throws Exception {
        Map<Long, HadoopVertex> vertices = generateGraph(ExampleGraph.TINKERGRAPH, new Configuration());

        assertEquals(asList(vertices.get(1l).getEdges(Direction.OUT, "knows")).size(), 2);
        assertEquals(asList(vertices.get(1l).getEdges(Direction.OUT, "created")).size(), 1);
        assertEquals(asList(vertices.get(1l).getEdges(Direction.OUT)).size(), 3);
        assertEquals(asList(vertices.get(1l).getEdges(Direction.OUT, "knows", "created")).size(), 3);
        assertEquals(asList(vertices.get(1l).getEdges(Direction.IN)).size(), 0);

        assertEquals(asList(vertices.get(2l).getEdges(Direction.IN, "knows")).size(), 1);
        assertEquals(asList(vertices.get(2l).getEdges(Direction.OUT, "created")).size(), 0);
        assertEquals(asList(vertices.get(2l).getEdges(Direction.OUT)).size(), 0);
        assertEquals(asList(vertices.get(2l).getEdges(Direction.IN)).size(), 1);

        assertEquals(asList(vertices.get(3l).getEdges(Direction.OUT, "knows")).size(), 0);
        assertEquals(asList(vertices.get(3l).getEdges(Direction.IN, "created")).size(), 3);
        assertEquals(asList(vertices.get(3l).getEdges(Direction.OUT)).size(), 0);
        assertEquals(asList(vertices.get(3l).getEdges(Direction.IN)).size(), 3);
        assertEquals(asList(vertices.get(3l).getEdges(Direction.IN, "knows", "created")).size(), 3);

        assertEquals(asList(vertices.get(4l).getEdges(Direction.BOTH, "created")).size(), 2);
        assertEquals(asList(vertices.get(4l).getEdges(Direction.BOTH)).size(), 3);
        assertEquals(asList(vertices.get(4l).getEdges(Direction.BOTH, "knows")).size(), 1);
        assertEquals(asList(vertices.get(4l).getEdges(Direction.BOTH, "knows", "created")).size(), 3);
        assertEquals(asList(vertices.get(4l).getEdges(Direction.BOTH, "blah")).size(), 0);
    }

    public void testNoPathsOnConstruction() throws Exception {
        noPaths(generateGraph(ExampleGraph.TINKERGRAPH, new Configuration()), Vertex.class);
        noPaths(generateGraph(ExampleGraph.TINKERGRAPH, new Configuration()), Edge.class);
    }

    public void testPropertyHandling() throws Exception {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 10l);
        assertEquals(vertex.getIdAsLong(), 10l);
        assertEquals(vertex.getPropertyKeys().size(), 0);
        vertex.setProperty("name", "marko");
        assertEquals(vertex.getProperties("name").iterator().next(), "marko");
        vertex.addProperty("name", "marko a. rodriguez");
        assertEquals(vertex.getPropertyKeys().size(), 1);
        Set<String> names = new HashSet<String>();
        Iterables.addAll(names, (Iterable) vertex.getProperties("name"));
        assertEquals(names.size(), 2);
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("marko a. rodriguez"));
        try {
            vertex.getProperty("name");
            fail();
        } catch (IllegalStateException e) {
        }
        int counter = 0;
        for (HadoopProperty property : vertex.getProperties()) {
            assertEquals(property.getName(), "name");
            assertTrue(property.getValue().equals("marko") || property.getValue().equals("marko a. rodriguez"));
            counter++;
        }
        assertEquals(counter, 2);

        ///////// BEGIN SERIALIZE

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        vertex.write(new DataOutputStream(bytes));
        vertex = new HadoopVertex(EmptyConfiguration.immutable(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        ///////// END SERIALIZE

        assertEquals(vertex.getIdAsLong(), 10l);
        assertEquals(vertex.getPropertyKeys().size(), 1);
        names = new HashSet<String>();
        Iterables.addAll(names, (Iterable) vertex.getProperties("name"));
        assertEquals(names.size(), 2);
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("marko a. rodriguez"));
        try {
            vertex.getProperty("name");
            fail();
        } catch (IllegalStateException e) {
        }
        counter = 0;
        for (HadoopProperty property : vertex.getProperties()) {
            assertEquals(property.getName(), "name");
            assertTrue(property.getValue().equals("marko") || property.getValue().equals("marko a. rodriguez"));
            counter++;
        }
        assertEquals(counter, 2);

    }

    // TODO: REGENERATE SEQUENCE FILE
    /*public void testSequenceFileRepresentation() throws Exception {
        final Configuration conf = new Configuration();
        final SequenceFile.Reader reader = new SequenceFile.Reader(
                FileSystem.get(conf), new Path(HadoopVertexTest.class.getResource("graph-of-the-gods-2.seq").toURI()), conf);
        NullWritable key = NullWritable.get();
        HadoopVertex value = new HadoopVertex();

        final Map<Long, HadoopVertex> graph = new HashMap<Long, HadoopVertex>();
        while (reader.next(key, value)) {
            graph.put(value.getIdAsLong(), value);
            value = new HadoopVertex();
        }
        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS_2);
        reader.close();
    }*/

    public void testLargeProperty() throws Exception {
        String value = "a24$%~bU*!";
        for (int i = 0; i < 18; i++) {
            value = value + value;
        }
        // a 2.6 million length string == ~5 books worth of data
        assertEquals(value.length(), 2621440);

        HadoopVertex vertex1 = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        vertex1.setProperty("name", value);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes));
        HadoopVertex vertex2 = new HadoopVertex(new EmptyConfiguration(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        assertEquals(vertex2.getProperty("name"), value);
    }
}