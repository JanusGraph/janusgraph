package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testutil.TestGraphConfigs;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TRIGGER_LOG;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanGraphBaseTest {

    public WriteConfiguration config;
    public StandardTitanGraph graph;
    public StoreFeatures features;
    public TitanTransaction tx;
    public TitanManagement mgmt;

    public Map<String,LogManager> logManagers;

    public TitanGraphBaseTest() {
    }

    public abstract WriteConfiguration getConfiguration();

    @Before
    public void setUp() throws Exception {
        this.config = getConfiguration();
        TestGraphConfigs.applyOverrides(config);
        Preconditions.checkNotNull(config);
        ModifiableConfiguration configuration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config.copy(), BasicConfiguration.Restriction.NONE);
        configuration.set(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX, "tmp");
        configuration.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "inst");
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        backend.clearStorage();
        open(config);
        logManagers = new HashMap<String,LogManager>();
    }

    public void open(WriteConfiguration config) {
        graph = (StandardTitanGraph) TitanFactory.open(config);
        features = graph.getConfiguration().getStoreFeatures();
        tx = graph.newTransaction();
        mgmt = graph.getManagementSystem();
    }

    @After
    public void tearDown() throws Exception {
        close();
        closeLogs();
    }

    public void finishSchema() {
        assert mgmt!=null;
        mgmt.commit();
        mgmt=graph.getManagementSystem();
        newTx();
        graph.commit();
    }

    public void close() {
        if (mgmt!=null && mgmt.isOpen()) mgmt.rollback();
        if (null != tx && tx.isOpen())
            tx.commit();


        if (null != graph)
            graph.shutdown();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        //tx = graph.newThreadBoundTransaction();
        tx = graph.newTransaction();
    }

    public void clopen(Object... settings) {
        config = getConfiguration();
        if (mgmt!=null && mgmt.isOpen()) mgmt.rollback();
        if (null != tx && tx.isOpen()) tx.commit();
        if (settings!=null && settings.length>0) {
            //Parse settings
            Preconditions.checkArgument(settings.length%2==0,"Expected even number of settings: %s",settings);
            Map<TestConfigOption,Object> options = Maps.newHashMap();
            for (int i=0;i<settings.length;i=i+2) {
                Preconditions.checkArgument(settings[i] instanceof TestConfigOption,"Expected configuration option but got: %s",settings[i]);
                Preconditions.checkNotNull(settings[i+1],"Null setting at position [%s]",i+1);
                options.put((TestConfigOption)settings[i],settings[i+1]);
            }
            TitanManagement gconf = graph.getManagementSystem();
            ModifiableConfiguration lconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.LOCAL);
            for (Map.Entry<TestConfigOption,Object> option : options.entrySet()) {
                if (option.getKey().option.isLocal()) {
                    lconf.set(option.getKey().option,option.getValue(),option.getKey().umbrella);
                } else {
                    gconf.set(ConfigElement.getPath(option.getKey().option,option.getKey().umbrella),option.getValue());
                }
            }
            gconf.commit();
            lconf.close();
        }
        if (null != graph && graph.isOpen())
            graph.shutdown();
        Preconditions.checkNotNull(config);
        open(config);
    }


    public static final TestConfigOption option(ConfigOption option, String... umbrella) {
        return new TestConfigOption(option,umbrella);
    }

    public static final class TestConfigOption {

        public final ConfigOption option;
        public final String[] umbrella;

        public TestConfigOption(ConfigOption option, String... umbrella) {
            Preconditions.checkNotNull(option);
            this.option = option;
            if (umbrella==null) umbrella=new String[0];
            this.umbrella = umbrella;
        }
    }

    /*
    ========= Log Helpers ============
     */

    private KeyColumnValueStoreManager logStoreManager = null;

    private void closeLogs() {
        try {
            for (LogManager lm : logManagers.values()) lm.close();
            logManagers.clear();
            if (logStoreManager!=null) {
                logStoreManager.close();
                logStoreManager=null;
            }
        } catch (StorageException e) {
            throw new TitanException(e);
        }
    }

    public void closeLogManager(String logManagerName) {
        if (logManagers.containsKey(logManagerName)) {
            try {
                logManagers.remove(logManagerName).close();
            } catch (StorageException e) {
                throw new TitanException("Could not close log manager " + logManagerName,e);
            }
        }
    }

    public Log openTriggerLog(String identifier, ReadMarker readMarker) {
        return openLog(TRIGGER_LOG, Backend.TRIGGER_LOG_PREFIX +identifier, readMarker);
    }

    public Log openTxLog(ReadMarker readMarker) {
        return openLog(TRANSACTION_LOG, Backend.SYSTEM_TX_LOG_NAME, readMarker);
    }

    private Log openLog(String logManagerName, String logName, ReadMarker readMarker) {
        try {
            ModifiableConfiguration configuration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config.copy(), BasicConfiguration.Restriction.NONE);
            configuration.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "reader");
            configuration.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, new StandardDuration(500L, TimeUnit.MILLISECONDS), logManagerName);
            if (logStoreManager==null) {
                logStoreManager = Backend.getStorageManager(configuration);
            }
            StoreFeatures f = logStoreManager.getFeatures();
            boolean part = f.isDistributed() && f.isKeyOrdered();
            configuration.set(GraphDatabaseConfiguration.IDS_PARTITION, part);
            assert logStoreManager!=null;
            if (!logManagers.containsKey(logManagerName)) {
                //Open log manager - only supports KCVSLog
                Configuration logConfig = configuration.restrictTo(logManagerName);
                Preconditions.checkArgument(logConfig.get(LOG_BACKEND).equals(LOG_BACKEND.getDefaultValue()));
                logManagers.put(logManagerName,new KCVSLogManager(logStoreManager,logConfig));
            }
            assert logManagers.containsKey(logManagerName);
            return logManagers.get(logManagerName).openLog(logName, readMarker);
        } catch (StorageException e) {
            throw new TitanException("Could not open log: "+ logName,e);
        }
    }

    /*
    ========= Type Definition Helpers ============
     */

    public TitanKey makeVertexIndexedKey(String name, Class datatype) {
        TitanKey key = mgmt.makeKey(name).dataType(datatype).cardinality(Cardinality.SINGLE).make();
        mgmt.createInternalIndex(name,Vertex.class,key);
        return key;
    }

    public TitanKey makeVertexIndexedUniqueKey(String name, Class datatype) {
        TitanKey key = mgmt.makeKey(name).dataType(datatype).cardinality(Cardinality.SINGLE).make();
        mgmt.createInternalIndex(name,Vertex.class,true,key);
        return key;
    }

    public void createExternalVertexIndex(TitanKey key, String backingIndex) {
        createExternalIndex(key,Vertex.class,backingIndex);
    }

    public void createExternalEdgeIndex(TitanKey key, String backingIndex) {
        createExternalIndex(key,Edge.class,backingIndex);
    }

    public TitanGraphIndex getExternalIndex(Class<? extends Element> clazz, String backingIndex) {
        String indexName = (Vertex.class.isAssignableFrom(clazz)?"v":"e")+backingIndex;
        TitanGraphIndex index = mgmt.getGraphIndex(indexName);
        if (index==null) {
            index = mgmt.createExternalIndex(indexName,clazz,backingIndex);
        }
        return index;
    }

    private void createExternalIndex(TitanKey key, Class<? extends Element> clazz, String backingIndex) {
        mgmt.addIndexKey(getExternalIndex(clazz,backingIndex),key);
    }

    public TitanKey makeKey(String name, Class datatype) {
        TitanKey key = mgmt.makeKey(name).dataType(datatype).cardinality(Cardinality.SINGLE).make();
        return key;
    }

    public TitanLabel makeLabel(String name) {
        return mgmt.makeLabel(name).make();
    }

    public TitanLabel makeKeyedEdgeLabel(String name, TitanKey sort, TitanKey signature) {
        TitanLabel relType = tx.makeLabel(name).
                sortKey(sort).signature(signature).directed().make();
        return relType;
    }

    /*
    ========= General Helpers ===========
     */

    public static final int DEFAULT_THREAD_COUNT = 4;

    public static int getThreadCount() {
        String s = System.getProperty("titan.test.threads");
        if (null != s)
            return Integer.valueOf(s);
        else
            return DEFAULT_THREAD_COUNT;
    }

    public static int wrapAround(int value, int maxValue) {
        value = value % maxValue;
        if (value < 0) value = value + maxValue;
        return value;
    }

}
