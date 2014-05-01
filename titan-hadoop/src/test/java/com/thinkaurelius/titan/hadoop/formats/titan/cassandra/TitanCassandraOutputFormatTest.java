package com.thinkaurelius.titan.hadoop.formats.titan.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusGraph;
import com.thinkaurelius.titan.hadoop.formats.TitanOutputFormatTest;
import com.thinkaurelius.titan.hadoop.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormatTest extends TitanOutputFormatTest {

    private static TitanGraph startUpCassandra() throws Exception {
        ModifiableConfiguration configuration = GraphDatabaseConfiguration.buildConfiguration();
        configuration.set(STORAGE_BACKEND,"embededcassandra");
        configuration.set(STORAGE_HOSTS,new String[]{"localhost"});
        configuration.set(STORAGE_CONF_FILE, TitanCassandraOutputFormat.class.getResource("cassandra.yaml").toString());
        configuration.set(DB_CACHE, false);
        configuration.set(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX, "tmp");
        configuration.set(UNIQUE_INSTANCE_ID, "inst");
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        backend.clearStorage();

        return TitanFactory.open(configuration);
    }

    private static BaseConfiguration getConfiguration() throws Exception {
        BaseConfiguration configuration = new BaseConfiguration();

        ModifiableConfiguration config = new ModifiableConfiguration(TITAN_NS,
                new CommonsConfiguration(configuration),
                BasicConfiguration.Restriction.NONE);

        config.set(STORAGE_BACKEND,"cassandrathrift");
        config.set(STORAGE_HOSTS,new String[]{"localhost"});
        config.set(DB_CACHE, false);

        return configuration;
    }


    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanCassandraOutputFormat.class.getPackage().getName() + ".*"));
    }

    public void testBulkLoading() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        super.testBulkLoading(getConfiguration(), f1);
    }

    public void testBulkElementDeletions() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkElementDeletions(getConfiguration(), f1, f2);
    }

    public void testFewElementDeletions() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testFewElementDeletions(getConfiguration(), f1, f2);
    }

    public void testBulkVertexPropertyDeletions() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkVertexPropertyDeletions(getConfiguration(), f1, f2);
    }

    public void testBulkVertexPropertyUpdates() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkVertexPropertyUpdates(getConfiguration(), f1, f2);
    }

    public void testBulkEdgeDerivations() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkEdgeDerivations(getConfiguration(), f1, f2);
    }

    public void testBulkEdgePropertyUpdates() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testBulkEdgePropertyUpdates(getConfiguration(), f1, f2);
    }

    public void testUnidirectionEdges() throws Exception {
        startUpCassandra();
        FaunusGraph f1 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("graphson-cassandra.properties"));
        FaunusGraph f2 = createFaunusGraph(TitanCassandraOutputFormat.class.getResourceAsStream("cassandra-cassandra.properties"));
        super.testUnidirectionEdges(getConfiguration(), f1, f2);
    }

}
