package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.CLIENT_ONLY;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.LOCAL_MODE;
import static com.thinkaurelius.titan.example.GraphOfTheGodsFactory.INDEX_NAME;
import static com.thinkaurelius.titan.graphdb.TitanGraphTest.evaluateQuery;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_CONF_FILE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.buildConfiguration;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import com.thinkaurelius.titan.diskstorage.es.ElasticsearchRunner;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.hadoop.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.tinkerpop.blueprints.Vertex;

public class CassandraESReindexTest extends TitanGraphBaseTest {

    private static final String ES_HOME = "../../titan-es";

    @BeforeClass
    public static void startES() {
        ElasticsearchRunner r = new ElasticsearchRunner(ES_HOME);
        r.stop();
        r.start();
    }

    @AfterClass
    public static void stopES() {
        new ElasticsearchRunner(ES_HOME).stop();
    }

    @Test
    public void testMixedIndexUpdatesWithReindex() throws Exception {
        Object[] settings = new Object[]{option(LOG_SEND_DELAY,MANAGEMENT_LOG),new StandardDuration(0, TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,MANAGEMENT_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,MANAGEMENT_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS)
        };

        clopen(settings);

        mgmt.makePropertyKey("desc").dataType(String.class).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        // Declare a mixed index
        TitanGraphIndex mixedIndex = mgmt.buildIndex("mixedTest",Vertex.class).addKey(name, Mapping.TEXT.getParameter()).buildMixedIndex(INDEX_NAME);
        finishSchema();

        // Create some named vertices
        for (int i=0;i<10;i++) {
            TitanVertex v = tx.addVertex();
            v.setProperty("name", "v"+i);
            v.setProperty("desc", "d"+i);
        }
        newTx();

        clopen(settings);

        // Check on one by name
        evaluateQuery(tx.query().has("name","v5"),
                ElementCategory.VERTEX,1,new boolean[]{false,true});
        newTx();

        finishSchema();
        mgmt.addIndexKey(mgmt.getGraphIndex("mixedTest"),mgmt.getPropertyKey("desc"));
        finishSchema();
        newTx();

        for (int i=20;i<30;i++) {
            TitanVertex v = tx.addVertex();
            v.setProperty("name", "v"+i);
            v.setProperty("desc", "d"+i);
        }
        newTx();

        // Should not yet be able to enable since not yet registered
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("mixedTest"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}

        // Register
//        mgmt.updateIndex(mgmt.getGraphIndex("mixedTest"), SchemaAction.REGISTER_INDEX);
        mgmt.commit();

        // Log propagation sleep (?)
        Thread.sleep(2000);
        finishSchema();

        // Enable index
        mgmt.updateIndex(mgmt.getGraphIndex("mixedTest"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        // Add more named vertices that should get mixed index entries
        for (int i=100;i<110;i++) {
            TitanVertex v = tx.addVertex();
            v.addProperty("name", "v"+i);
            v.setProperty("desc", "d"+i);
        }
        newTx();

        Thread.sleep(10000L);

        // Check that we only see new data
        evaluateQuery(tx.query().has("desc", Text.CONTAINS, "d5"),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"mixedTest");
        evaluateQuery(tx.query().has("desc", Text.CONTAINS, "d105"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"mixedTest");
        newTx();

        // Run a reindex job
        Properties titanInputProperties = new Properties();
        titanInputProperties.setProperty(ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_BACKEND), "cassandrathrift");
        String ks = getClass().getSimpleName();
        titanInputProperties.setProperty(ConfigElement.getPath(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE), cleanKeyspaceName(ks));
        titanInputProperties.setProperty(ConfigElement.getPath(CassandraThriftStoreManager.CPOOL_MAX_TOTAL), "1");
        titanInputProperties.setProperty(ConfigElement.getPath(CassandraThriftStoreManager.CPOOL_MAX_ACTIVE), "1");
        titanInputProperties.setProperty(ConfigElement.getPath(CassandraThriftStoreManager.CPOOL_MAX_IDLE), "1");
        titanInputProperties.setProperty(ConfigElement.getPath(GraphDatabaseConfiguration.INDEX_BACKEND, "search"), "elasticsearch");
        // External ES, must be started manually before tests and cleaned afterward
        titanInputProperties.setProperty(ConfigElement.getPath(ElasticSearchIndex.CLIENT_ONLY, "search"), "true");
        titanInputProperties.setProperty(ConfigElement.getPath(ElasticSearchIndex.LOCAL_MODE, "search"), "false");
        // Embedded ES -- plays badly with MR
        //titanInputProperties.setProperty("index.search.directory", "es");
        //titanInputProperties.setProperty("index.search.client-only", "false");
        //titanInputProperties.setProperty("index.search.local-mode", "true");
        TitanIndexRepair.cassandraRepair(titanInputProperties, "mixedTest", "", "org.apache.cassandra.dht.Murmur3Partitioner");
        newTx();

        // Use index, see old and new data
        evaluateQuery(tx.query().has("desc", Text.CONTAINS, "d5"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"mixedTest");
        evaluateQuery(tx.query().has("desc", Text.CONTAINS, "d105"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"mixedTest");
    }

    // Need to make CassandraStorageSetup's static initializer play nice with titan-hadoop-parent before that code can be used here

    @Override
    public WriteConfiguration getConfiguration() {
        String ks = getClass().getSimpleName();
        ModifiableConfiguration config = buildConfiguration();
        config.set(STORAGE_BACKEND,"embeddedcassandra");
        config.set(STORAGE_HOSTS,new String[]{"localhost"});
        config.set(STORAGE_CONF_FILE, TitanCassandraOutputFormat.class.getResource("cassandra.yaml").toString());
        config.set(CASSANDRA_KEYSPACE, cleanKeyspaceName(ks));
        config.set(PAGE_SIZE,500);
        config.set(INDEX_BACKEND, "elasticsearch", INDEX_NAME);
        // External ES, must be started manually before tests and cleaned afterward
        config.set(LOCAL_MODE, false, INDEX_NAME);
        config.set(CLIENT_ONLY, true, INDEX_NAME);
        // Embedded ES -- plays badly with MR
//        config.set(INDEX_DIRECTORY, "es", INDEX_NAME);
//        config.set(LOCAL_MODE, true, INDEX_NAME);
//        config.set(CLIENT_ONLY, false, INDEX_NAME);
        return config.getConfiguration();
    }

    public static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("[^a-zA-Z_]")) {
            return "strhash" + String.valueOf(Math.abs(raw.hashCode()));
        } else {
            return raw;
        }
    }
}
