package com.thinkaurelius.faunus.formats.titan.hbase;

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
public class TitanHBaseOutputFormatTest extends TitanOutputFormatTest {

    private static TitanGraph startUpHBase() throws Exception {
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("storage.backend", "hbase");
        configuration.setProperty("storage.hostname", "localhost");
        configuration.setProperty("storage.port", "2181");
        configuration.setProperty("storage.db-cache", "false");
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(configuration);
        graphconfig.getBackend().clearStorage();
        return TitanFactory.open(configuration);
    }

    private static BaseConfiguration getConfiguration() throws Exception {
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("storage.backend", "hbase");
        configuration.setProperty("storage.hostname", "localhost");
        configuration.setProperty("storage.port", "2181");
        configuration.setProperty("storage.db-cache", "false");
        return configuration;
    }


    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanHBaseOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        super.testBulkLoading(getConfiguration(), f1);
    }

    public void testBulkElementDeletions() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkElementDeletions(getConfiguration(), f1, f2);
    }

    public void testFewElementDeletions() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testFewElementDeletions(getConfiguration(), f1, f2);
    }

    public void testBulkVertexPropertyDeletions() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkVertexPropertyDeletions(getConfiguration(), f1, f2);
    }

    public void testBulkVertexPropertyUpdates() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkVertexPropertyUpdates(getConfiguration(), f1, f2);
    }

    public void testBulkEdgeDerivations() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkEdgeDerivations(getConfiguration(), f1, f2);
    }

    public void testBulkEdgePropertyUpdates() throws Exception {
        startUpHBase();
        FaunusGraph f1 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.testBulkEdgePropertyUpdates(getConfiguration(), f1, f2);
    }
}
