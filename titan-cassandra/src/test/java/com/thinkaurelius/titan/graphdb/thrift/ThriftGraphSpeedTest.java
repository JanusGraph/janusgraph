package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.graphdb.SpeedTestSchema;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.graphdb.TitanGraphSpeedTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

@Category({PerformanceTests.class})
public class ThriftGraphSpeedTest extends TitanGraphSpeedTest {

    private static StandardTitanGraph graph;
    private static SpeedTestSchema schema;

    private static final Logger log =
            LoggerFactory.getLogger(ThriftGraphSpeedTest.class);

    public ThriftGraphSpeedTest() throws BackendException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ThriftGraphSpeedTest.class.getSimpleName()));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    protected StandardTitanGraph getGraph() throws BackendException {


        if (null == graph) {
            GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
            graphconfig.getBackend().clearStorage();
            log.debug("Cleared backend storage");
            graph = (StandardTitanGraph)TitanFactory.open(conf);
            initializeGraph(graph);
        }
        return graph;
    }

    @Override
    protected SpeedTestSchema getSchema() {
        if (null == schema) {
            schema = SpeedTestSchema.get();
        }
        return schema;
    }
}