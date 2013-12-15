package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.formats.TitanOutputFormatTest;
import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import org.apache.commons.configuration.BaseConfiguration;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormatTest extends TitanOutputFormatTest {

    private static TitanGraph generateTitanGraph() throws Exception {
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("storage.backend", "embeddedcassandra");
        configuration.setProperty("storage.hostname", "localhost");
        configuration.setProperty("storage.cassandra-config-dir", TitanCassandraOutputFormat.class.getResource("cassandra.yaml").toString());
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(configuration);
        graphconfig.getBackend().clearStorage();
        return TitanFactory.open(configuration);
    }

    private void bulkLoadGraphOfTheGods() throws Exception {
        FaunusGraph f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        new FaunusPipeline(f)._().submit();
    }


    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanCassandraOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        TitanGraph g = generateTitanGraph();
        bulkLoadGraphOfTheGods();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        new GremlinPipeline(g).V().sideEffect(new PipeFunction<Vertex, Vertex>() {
            @Override
            public Vertex compute(Vertex vertex) {
                assertEquals(2, vertex.getPropertyKeys().size());
                assertNotNull(vertex.getProperty("name"));
                return vertex;
            }
        }).iterate();
        assertEquals("saturn", new GremlinPipeline(g).V("name", "hercules").out("father").out("father").property("name").next());
        List names = new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).inV().property("name").toList();
        assertTrue(names.contains("nemean"));
        assertTrue(names.contains("hydra"));
        assertTrue(names.contains("cerberus"));
    }

    public void testBulkElementDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        bulkLoadGraphOfTheGods();

        FaunusGraph f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        new FaunusPipeline(f).V().drop().submit();

        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        bulkLoadGraphOfTheGods();
        f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        new FaunusPipeline(f).E().drop().submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
    }

    public void testBulkPropertyDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        bulkLoadGraphOfTheGods();

        FaunusGraph f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        new FaunusPipeline(f).V().sideEffect("{it.removeProperty('name')}").submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        for (Vertex v : g.getVertices()) {
            assertNull(v.getProperty("name"));
            assertEquals(1, v.getPropertyKeys().size());
        }
        new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).iterate();
    }

}
