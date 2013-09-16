package com.thinkaurelius.titan.graphdb.thrift;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.GroovySerialTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testcategory.RandomPartitionerTests;
import com.thinkaurelius.titan.testutil.GraphGenerator;

@Category({RandomPartitionerTests.class})
public class InternalCassandraGraphSerialTest extends GroovySerialTest {
    
    private static StandardTitanGraph graph; 
    private static GraphGenerator gen;
    
    public InternalCassandraGraphSerialTest() throws StorageException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphSerialTest.class.getSimpleName()));
    }
    
    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
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
    protected GraphGenerator getGenerator() {
        if (null == gen) {
            gen = new GraphGenerator(VERTEX_PROP_COUNT, EDGE_PROP_COUNT);
        }
        return gen;
    }

    private void initializeGraph(TitanGraph g) throws StorageException {
        System.out.println("Initializing graph");
        GraphGenerator gg = getGenerator();
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
        graphconfig.getBackend().clearStorage();
        gg.generate(g, VERTEX_COUNT, EDGE_COUNT);
        System.out.println("Initialized graph");
    }
}