package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertexTest extends BaseTest {

    public void testRawComparator() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(10);
        FaunusVertex vertex2 = new FaunusVertex(11);

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        vertex2.write(new DataOutputStream(bytes2));

        assertEquals(-1, new FaunusVertex.Comparator().compare(bytes1.toByteArray(), 0, bytes1.size(), bytes2.toByteArray(), 0, bytes2.size()));
        assertEquals(1, new FaunusVertex.Comparator().compare(bytes2.toByteArray(), 0, bytes2.size(), bytes1.toByteArray(), 0, bytes1.size()));
        assertEquals(0, new FaunusVertex.Comparator().compare(bytes1.toByteArray(), 0, bytes1.size(), bytes1.toByteArray(), 0, bytes1.size()));

        System.out.println("Vertex with 0 properties has a byte size of: " + bytes1.toByteArray().length);
    }

    public void testSimpleSerialization() throws IOException {

        FaunusVertex vertex1 = new FaunusVertex(10l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        vertex1.write(out);

        // id length is 1 byte (variable long)
        // pathsEnabled boolean 1 byte (boolean)
        // paths size 1 byte (variable int)
        // properties size 1 byte (variable int)
        // out edge types size 1 byte (variable int)
        // in edge types size 1 byte (variable int)
        assertEquals(bytes.toByteArray().length, 6);
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        System.out.println("Vertex with 0 properties has a byte size of: " + bytes.toByteArray().length);

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertFalse(vertex1.hasPaths());
        assertFalse(vertex2.hasPaths());
        assertEquals(vertex1.pathCount(), 0);
        assertEquals(vertex2.pathCount(), 0);
        assertFalse(vertex1.pathEnabled);
        assertFalse(vertex2.pathEnabled);
        assertFalse(vertex2.getEdges(Direction.OUT).iterator().hasNext());
        assertFalse(vertex2.getEdges(Direction.IN).iterator().hasNext());
        assertFalse(vertex2.getEdges(Direction.BOTH).iterator().hasNext());
        assertEquals(vertex2.getPropertyKeys().size(), 0);

    }

    public void testVertexSerialization() throws IOException {

        FaunusVertex vertex1 = new FaunusVertex(10);
        vertex1.addEdge(OUT, new FaunusEdge(vertex1.getIdAsLong(), 2, "knows"));
        vertex1.addEdge(OUT, new FaunusEdge(vertex1.getIdAsLong(), 3, "knows"));
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);
        vertex1.setProperty("longitude", 10.01d);
        vertex1.setProperty("latitude", 11.399f);
        vertex1.setProperty("size", 10l);
        vertex1.setProperty("boolean", true);
        assertEquals(vertex1.getPropertyKeys().size(), 6);
        assertEquals(vertex1.getProperty("name"), "marko");
        assertEquals(vertex1.getProperty("age"), 32);
        assertEquals(vertex1.getProperty("longitude"), 10.01d);
        assertEquals(vertex1.getProperty("latitude"), 11.399f);
        assertEquals(vertex1.getProperty("size"), 10l);
        assertTrue((Boolean) vertex1.getProperty("boolean"));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        vertex1.write(new DataOutputStream(bytes));
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        System.out.println("Vertex with 6 properties and 2 outgoing edges has a byte size of: " + bytes.toByteArray().length);

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertEquals(vertex2.getPropertyKeys().size(), 6);
        assertEquals(vertex2.getProperty("name"), "marko");
        assertEquals(vertex2.getProperty("age"), 32);
        assertEquals(vertex2.getProperty("longitude"), 10.01d);
        assertEquals(vertex2.getProperty("latitude"), 11.399f);
        assertEquals(vertex1.getProperty("size"), 10l);
        assertTrue((Boolean) vertex2.getProperty("boolean"));
        Iterator<Edge> edges = vertex2.getEdges(Direction.OUT).iterator();
        assertTrue(edges.hasNext());
        assertEquals(edges.next().getLabel(), "knows");
        assertTrue(edges.hasNext());
        assertEquals(edges.next().getLabel(), "knows");
        assertFalse(edges.hasNext());

    }

    public void testVertexSerializationWithPaths() throws IOException {

        FaunusVertex vertex1 = new FaunusVertex(10);
        vertex1.enablePath(true); // TODO: look this all over
        vertex1.addEdge(OUT, new FaunusEdge(vertex1.getIdAsLong(), 2, "knows"));
        vertex1.addEdge(IN, new FaunusEdge(3, vertex1.getIdAsLong(), "knows"));
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
        vertex1.addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(10l), new FaunusVertex.MicroVertex(1l)), false);
        vertex1.addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(10l), new FaunusVertex.MicroVertex(2l)), false);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        vertex1.write(new DataOutputStream(bytes));
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
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
        for (List<FaunusElement.MicroElement> path : vertex2.getPaths()) {
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
        FaunusVertex vertex1 = new FaunusVertex(1l);
        vertex1.addEdge(OUT, new FaunusEdge(vertex1.getIdAsLong(), vertex1.getIdAsLong(), "knows"));

        assertNull(vertex1.getProperty("name"));
        assertNull(vertex1.removeProperty("name"));
        assertEquals(vertex1.getPropertyKeys().size(), 0);
        assertEquals(vertex1.getProperties().size(), 0);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes));
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

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
        FaunusVertex vertex = new FaunusVertex(1l);
        vertex.setProperty("name", "marko");
        vertex.addEdge(OUT, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(OUT, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        assertEquals(asList(vertex.getEdges(OUT)).size(), 2);
        vertex.removeEdges(Tokens.Action.DROP, OUT, "knows");
        assertEquals(asList(vertex.getEdges(OUT)).size(), 1);
        assertEquals(vertex.getEdges(OUT).iterator().next().getLabel(), "created");
        assertEquals(vertex.getProperty("name"), "marko");

        vertex = new FaunusVertex(1l);
        vertex.setProperty("name", "marko");
        vertex.addEdge(OUT, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(OUT, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        vertex.addEdge(IN, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(IN, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        assertEquals(asList(vertex.getEdges(OUT)).size(), 2);
        vertex.removeEdges(Tokens.Action.DROP, BOTH, "knows");
        assertEquals(asList(vertex.getEdges(BOTH)).size(), 2);
        assertEquals(vertex.getEdges(OUT).iterator().next().getLabel(), "created");
        assertEquals(vertex.getProperty("name"), "marko");

        vertex = new FaunusVertex(1l);
        vertex.setProperty("name", "marko");
        vertex.addEdge(OUT, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "knows"));
        vertex.addEdge(OUT, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        vertex.addEdge(IN, new FaunusEdge(vertex.getIdAsLong(), vertex.getIdAsLong(), "created"));
        assertEquals(asList(vertex.getEdges(OUT)).size(), 2);
        vertex.removeEdges(Tokens.Action.KEEP, BOTH, "knows");
        assertEquals(asList(vertex.getEdges(OUT)).size(), 1);
        assertEquals(vertex.getEdges(OUT).iterator().next().getLabel(), "knows");
        assertEquals(vertex.getProperty("name"), "marko");
    }

    public void testGetVerticesAndQuery() throws Exception {
        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, new Configuration());
        for (FaunusVertex vertex : graph.values()) {
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
        Map<Long, FaunusVertex> vertices = generateGraph(ExampleGraph.TINKERGRAPH, new Configuration());

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

    public void testVertexReuse() throws Exception {
        FaunusVertex vertex = new FaunusVertex();
        assertEquals(vertex.getId(), -1l);
        assertFalse(vertex.pathEnabled);
        assertFalse(vertex.getEdges(BOTH).iterator().hasNext());
        vertex.addEdge(OUT, new FaunusEdge(1, 2, "knows"));
        assertTrue(vertex.getEdges(BOTH).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 0);
        vertex.setProperty("name", "marko");
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getPropertyKeys().size(), 1);

        vertex.reuse(10);
        assertEquals(vertex.getId(), 10l);
        assertFalse(vertex.getEdges(BOTH).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 0);
        assertNull(vertex.getProperty("name"));
        assertFalse(vertex.pathEnabled);

        vertex.enablePath(true);
        assertTrue(vertex.pathEnabled);
        assertEquals(vertex.getPaths().size(), 0);

        vertex.reuse(220);
        assertEquals(vertex.getId(), 220l);
        assertFalse(vertex.getEdges(BOTH).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 0);
        assertNull(vertex.getProperty("name"));
        assertTrue(vertex.pathEnabled);
        vertex.startPath();
        assertEquals(vertex.getPaths().size(), 1);
        assertEquals(vertex.getPaths().get(0).size(), 1);
        assertEquals(vertex.getPaths().get(0).get(0).getId(), 220);

        vertex.reuse(34);
        assertTrue(vertex.pathEnabled);
        assertEquals(vertex.getPaths().size(), 0);
        vertex.startPath();
        assertEquals(vertex.getPaths().size(), 1);
        assertEquals(vertex.getPaths().get(0).size(), 1);
        assertEquals(vertex.getPaths().get(0).get(0).getId(), 34);
    }

    public void testNoPathsOnConstruction() throws Exception {
        noPaths(generateGraph(ExampleGraph.TINKERGRAPH, new Configuration()), Vertex.class);
        noPaths(generateGraph(ExampleGraph.TINKERGRAPH, new Configuration()), Edge.class);
    }

    public void testSequenceFileRepresentation() throws Exception {
        final Configuration conf = new Configuration();
        final SequenceFile.Reader reader = new SequenceFile.Reader(
                FileSystem.get(conf), new Path(FaunusVertexTest.class.getResource("graph-of-the-gods-2.seq").toURI()), conf);
        NullWritable key = NullWritable.get();
        FaunusVertex value = new FaunusVertex();

        final Map<Long, FaunusVertex> graph = new HashMap<Long, FaunusVertex>();
        while (reader.next(key, value)) {
            graph.put(value.getIdAsLong(), value);
            value = new FaunusVertex();
        }
        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS_2);
        reader.close();
    }
}