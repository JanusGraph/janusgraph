package com.thinkaurelius.titan.hadoop.formats.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.formats.TitanOutputFormatTest;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import org.apache.commons.configuration.BaseConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseOutputFormatTest extends TitanOutputFormatTest {

    private static TitanGraph startUpHBase() throws Exception {
        ModifiableConfiguration configuration = GraphDatabaseConfiguration.buildConfiguration();
        configuration.set(STORAGE_BACKEND,"hbase");
        configuration.set(STORAGE_HOSTS,new String[]{"localhost"});
        configuration.set(STORAGE_PORT,2181);
        configuration.set(DB_CACHE, false);
        configuration.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        configuration.set(UNIQUE_INSTANCE_ID, "inst");
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        backend.clearStorage();

        return TitanFactory.open(configuration);
    }

    private static BaseConfiguration getConfiguration() throws Exception {
        BaseConfiguration configuration = new BaseConfiguration();

        ModifiableConfiguration config = new ModifiableConfiguration(ROOT_NS,
                new CommonsConfiguration(configuration),
                BasicConfiguration.Restriction.NONE);

        config.set(STORAGE_BACKEND,"hbase");
        config.set(STORAGE_HOSTS,new String[]{"localhost"});
        config.set(STORAGE_PORT,2181);
        config.set(DB_CACHE, false);

        return configuration;
    }


    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanHBaseOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        super.doBulkLoading(getConfiguration(), f1);
    }

    public void testBulkElementDeletions() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        HadoopGraph f2 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.doBulkElementDeletions(getConfiguration(), f1, f2);
    }

    public void testFewElementDeletions() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        HadoopGraph f2 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.doFewElementDeletions(getConfiguration(), f1, f2);
    }

    public void testBulkVertexPropertyDeletions() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        HadoopGraph f2 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.doBulkVertexPropertyDeletions(getConfiguration(), f1, f2);
    }

    public void testBulkVertexPropertyUpdates() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        HadoopGraph f2 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.doBulkVertexPropertyUpdates(getConfiguration(), f1, f2);
    }

    public void testBulkEdgeDerivations() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        HadoopGraph f2 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.doBulkEdgeDerivations(getConfiguration(), f1, f2);
    }

    public void testBulkEdgePropertyUpdates() throws Exception {
        startUpHBase();
        HadoopGraph f1 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("graphson-hbase.properties"));
        HadoopGraph f2 = createHadoopGraph(TitanHBaseOutputFormat.class.getResourceAsStream("hbase-hbase.properties"));
        super.doBulkEdgePropertyUpdates(getConfiguration(), f1, f2);
    }
}
