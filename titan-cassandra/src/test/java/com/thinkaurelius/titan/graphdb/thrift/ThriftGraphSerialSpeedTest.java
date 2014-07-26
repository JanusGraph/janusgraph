package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.graphdb.TitanGraphSerialSpeedTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.thinkaurelius.titan.testutil.gen.Schema;

@Category({PerformanceTests.class})
public class ThriftGraphSerialSpeedTest extends TitanGraphSerialSpeedTest {

    private static StandardTitanGraph graph;
    private static Schema schema;

    private static final Logger log =
            LoggerFactory.getLogger(ThriftGraphSerialSpeedTest.class);

    public ThriftGraphSerialSpeedTest() throws BackendException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ThriftGraphSerialSpeedTest.class.getSimpleName()));
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
    protected Schema getSchema() {
        if (null == schema) {
            schema = new Schema.Builder(VERTEX_COUNT, EDGE_COUNT).build();
        }
        return schema;
    }
}