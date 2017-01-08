package org.janusgraph.graphdb.thrift;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.SpeedTestSchema;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.JanusGraphSpeedTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.testcategory.PerformanceTests;

@Category({PerformanceTests.class})
public class ThriftGraphSpeedTest extends JanusGraphSpeedTest {

    private static StandardJanusGraph graph;
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
    protected StandardJanusGraph getGraph() throws BackendException {


        if (null == graph) {
            GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
            graphconfig.getBackend().clearStorage();
            log.debug("Cleared backend storage");
            graph = (StandardJanusGraph)JanusGraphFactory.open(conf);
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
