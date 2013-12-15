package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.formats.TitanOutputFormatTest;
import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
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
        deleteDirectory("target/cassandra");
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("storage.backend", "embeddedcassandra");
        configuration.setProperty("storage.hostname", "localhost");
        configuration.setProperty("storage.cassandra-config-dir", TitanCassandraOutputFormat.class.getResource("cassandra.yaml").toString());
        return TitanFactory.open(configuration);
    }

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanCassandraOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        new FaunusPipeline(f)._().submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        for (Vertex v : g.getVertices()) {
            assertNotNull(v.getProperty("name"));
            assertEquals(v.getPropertyKeys().size(), 2);
        }
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

    public void testBulkDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        new FaunusPipeline(f)._().submit();
        f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        new FaunusPipeline(f).V().drop().submit();

        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        new FaunusPipeline(f)._().submit();
        f = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        new FaunusPipeline(f).E().drop().submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
    }

}
