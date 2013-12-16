package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.formats.TitanOutputFormatTest;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseOutputFormatTest extends TitanOutputFormatTest {

    private static TitanGraph generateTitanGraph() throws Exception {
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("storage.backend", "hbase");
        configuration.setProperty("storage.hostname", "localhost");
        configuration.setProperty("storage.port", "2181");
        configuration.setProperty("storage.db-cache","false");
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(configuration);
        graphconfig.getBackend().clearStorage();
        return TitanFactory.open(configuration);
    }


    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanHBaseOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        super.testBulkLoading(g, f1);
    }

    public void testBulkElementDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkElementDeletions(g, f1, f2);
    }

    public void testFewElementDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testFewElementDeletions(g, f1, f2);
    }

    public void testBulkVertexPropertyDeletions() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkVertexPropertyDeletions(g, f1, f2);
    }

    public void testBulkVertexPropertyUpdates() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkVertexPropertyUpdates(g, f1, f2);
    }

    public void testBulkEdgeDerivations() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkEdgeDerivations(g, f1, f2);
    }

    public void testBulkEdgePropertyUpdates() throws Exception {
        TitanGraph g = generateTitanGraph();
        FaunusGraph f1 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = generateFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkEdgePropertyUpdates(g, f1, f2);
    }
}
