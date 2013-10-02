package com.thinkaurelius.titan.graphdb.thrift;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphSerialSpeedTest;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.thinkaurelius.titan.testutil.gen.Schema;

@Category({PerformanceTests.class})
public class InternalCassandraGraphSerialSpeedTest extends TitanGraphSerialSpeedTest {
    
    private static StandardTitanGraph graph; 
    private static Schema schema;
    
    public InternalCassandraGraphSerialSpeedTest() throws StorageException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphSerialSpeedTest.class.getSimpleName()));
    }
    
    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }
    
    @Override
    protected StandardTitanGraph getGraph() throws StorageException {
        if (null == graph) {
            graph = (StandardTitanGraph)TitanFactory.open(conf);
            initializeGraph(graph);
        }
        return graph;
    }
    
    @Override
    protected Schema getSchema() {
        if (null == schema) {
            schema = new Schema.Builder(VERTEX_COUNT, EDGE_COUNT).build();
        }
        return schema;
    }
}