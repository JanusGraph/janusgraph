package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.formats.TitanOutputFormatTest;
import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;

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


    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanCassandraOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        super.testBulkLoading(g, f1);
    }

    public void testBulkElementDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkElementDeletions(g, f1, f2);

    }

    public void testBulkVertexPropertyDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkVertexPropertyDeletions(g, f1, f2);
    }

    public void testBulkVertexPropertyUpdates() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkVertexPropertyUpdates(g, f1, f2);
    }

    public void testBulkEdgeDerivations() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkEdgeDerivations(g, f1, f2);
    }

    public void testBulkEdgePropertyUpdates() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkEdgePropertyUpdates(g, f1, f2);
    }

}
