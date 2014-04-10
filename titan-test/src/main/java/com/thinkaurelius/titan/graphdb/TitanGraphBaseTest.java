package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testutil.TestGraphConfigs;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanGraphBaseTest {

    public WriteConfiguration config;
    public StandardTitanGraph graph;
    public StoreFeatures features;
    public TitanTransaction tx;
    public TitanManagement mgmt;

    public TitanGraphBaseTest() {
    }

    public abstract WriteConfiguration getConfiguration();

    @Before
    public void setUp() throws Exception {
        this.config = getConfiguration();
        TestGraphConfigs.applyOverrides(config);
        Preconditions.checkNotNull(config);
        ModifiableConfiguration configuration = new ModifiableConfiguration(GraphDatabaseConfiguration.TITAN_NS,config.copy(), BasicConfiguration.Restriction.NONE);
        configuration.set(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX, "tmp");
        configuration.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "inst");
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        backend.clearStorage();
        open(config);
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
    }

    public void finishSchema() {
        assert mgmt!=null;
        mgmt.commit();
        mgmt=graph.getManagementSystem();
        newTx();
        graph.commit();
    }

    public void close() {
        if (mgmt!=null) mgmt.rollback();
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
        if (mgmt!=null) mgmt.rollback();
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
            ModifiableConfiguration lconf = new ModifiableConfiguration(GraphDatabaseConfiguration.TITAN_NS,config, BasicConfiguration.Restriction.LOCAL);
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
