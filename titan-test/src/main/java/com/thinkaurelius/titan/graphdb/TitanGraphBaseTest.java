package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.BackendException;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.thinkaurelius.titan.testutil.TestGraphConfigs;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanGraphBaseTest {

    public static final String LABEL_NAME = T.label.getAccessor();
    public static final String ID_NAME = T.id.getAccessor();

    public WriteConfiguration config;
    public BasicConfiguration readConfig;
    public StandardTitanGraph graph;
    public StoreFeatures features;
    public TitanTransaction tx;
    public TitanManagement mgmt;

    public Map<String,LogManager> logManagers;

    public TitanGraphBaseTest() {
    }

    public abstract WriteConfiguration getConfiguration();

    public Configuration getConfig() {
        return new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,config.copy(), BasicConfiguration.Restriction.NONE);
    }

    public static void clearGraph(WriteConfiguration config) throws BackendException {
        ModifiableConfiguration adjustedConfig = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config.copy(), BasicConfiguration.Restriction.NONE);
        adjustedConfig.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        adjustedConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "inst");
        Backend backend = new Backend(adjustedConfig);
        backend.initialize(adjustedConfig);
        backend.clearStorage();
    }

    @Before
    public void setUp() throws Exception {
        this.config = getConfiguration();
        TestGraphConfigs.applyOverrides(config);
        Preconditions.checkNotNull(config);
        clearGraph(config);
        readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        open(config);
        logManagers = new HashMap<String,LogManager>();
    }

    public void open(WriteConfiguration config) {
        graph = (StandardTitanGraph) TitanFactory.open(config);
        features = graph.getConfiguration().getStoreFeatures();
        tx = graph.newTransaction();
        mgmt = graph.openManagement();
    }

    @After
    public void tearDown() throws Exception {
        close();
        closeLogs();
    }

    public void finishSchema() {
        if (mgmt!=null && mgmt.isOpen())
            mgmt.commit();
        mgmt=graph.openManagement();
        newTx();
        graph.tx().commit();
    }

    public void close() {
        if (mgmt!=null && mgmt.isOpen()) mgmt.rollback();
        if (null != tx && tx.isOpen())
            tx.commit();


        if (null != graph && graph.isOpen())
            graph.close();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        //tx = graph.newThreadBoundTransaction();
        tx = graph.newTransaction();
    }

    public static Map<TestConfigOption,Object> validateConfigOptions(Object... settings) {
        //Parse settings
        Preconditions.checkArgument(settings.length%2==0,"Expected even number of settings: %s",settings);
        Map<TestConfigOption,Object> options = Maps.newHashMap();
        for (int i=0;i<settings.length;i=i+2) {
            Preconditions.checkArgument(settings[i] instanceof TestConfigOption,"Expected configuration option but got: %s",settings[i]);
            Preconditions.checkNotNull(settings[i+1],"Null setting at position [%s]",i+1);
            options.put((TestConfigOption)settings[i],settings[i+1]);
        }
        return options;
    }

    public void clopen(Object... settings) {
        config = getConfiguration();
        if (mgmt!=null && mgmt.isOpen()) mgmt.rollback();
        if (null != tx && tx.isOpen()) tx.commit();
        if (settings!=null && settings.length>0) {
            Map<TestConfigOption,Object> options = validateConfigOptions(settings);
            TitanManagement gconf = null;
            ModifiableConfiguration lconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.LOCAL);
            for (Map.Entry<TestConfigOption,Object> option : options.entrySet()) {
                if (option.getKey().option.isLocal()) {
                    lconf.set(option.getKey().option,option.getValue(),option.getKey().umbrella);
                } else {
                    if (gconf==null) gconf = graph.openManagement();
                    gconf.set(ConfigElement.getPath(option.getKey().option,option.getKey().umbrella),option.getValue());
                }
            }
            if (gconf!=null) gconf.commit();
            lconf.close();
        }
        if (null != graph && graph.isOpen())
            graph.close();
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
        } catch (BackendException e) {
            throw new TitanException(e);
        }
    }

    public void closeLogManager(String logManagerName) {
        if (logManagers.containsKey(logManagerName)) {
            try {
                logManagers.remove(logManagerName).close();
            } catch (BackendException e) {
                throw new TitanException("Could not close log manager " + logManagerName,e);
            }
        }
    }

    public Log openUserLog(String identifier) {
        return openLog(USER_LOG, GraphDatabaseConfiguration.USER_LOG_PREFIX +identifier);
    }

    public Log openTxLog() {
        return openLog(TRANSACTION_LOG, Backend.SYSTEM_TX_LOG_NAME);
    }

    private Log openLog(String logManagerName, String logName) {
        try {
            ModifiableConfiguration configuration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config.copy(), BasicConfiguration.Restriction.NONE);
            configuration.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "reader");
            configuration.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(500L), logManagerName);
            if (logStoreManager==null) {
                logStoreManager = Backend.getStorageManager(configuration);
            }
            StoreFeatures f = logStoreManager.getFeatures();
            boolean part = f.isDistributed() && f.isKeyOrdered();
            if (part) {
                for (String logname : new String[]{USER_LOG,TRANSACTION_LOG,MANAGEMENT_LOG})
                configuration.set(KCVSLogManager.LOG_MAX_PARTITIONS,8,logname);
            }
            assert logStoreManager!=null;
            if (!logManagers.containsKey(logManagerName)) {
                //Open log manager - only supports KCVSLog
                Configuration logConfig = configuration.restrictTo(logManagerName);
                Preconditions.checkArgument(logConfig.get(LOG_BACKEND).equals(LOG_BACKEND.getDefaultValue()));
                logManagers.put(logManagerName,new KCVSLogManager(logStoreManager,logConfig));
            }
            assert logManagers.containsKey(logManagerName);
            return logManagers.get(logManagerName).openLog(logName);
        } catch (BackendException e) {
            throw new TitanException("Could not open log: "+ logName,e);
        }
    }

    /*
    ========= Schema Type Definition Helpers ============
     */

    public PropertyKey makeVertexIndexedKey(String name, Class datatype) {
        PropertyKey key = mgmt.makePropertyKey(name).dataType(datatype).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex(name,Vertex.class).addKey(key).buildCompositeIndex();
        return key;
    }

    public PropertyKey makeVertexIndexedUniqueKey(String name, Class datatype) {
        PropertyKey key = mgmt.makePropertyKey(name).dataType(datatype).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex(name,Vertex.class).addKey(key).unique().buildCompositeIndex();
        return key;
    }

    public void createExternalVertexIndex(PropertyKey key, String backingIndex) {
        createExternalIndex(key,Vertex.class,backingIndex);
    }

    public void createExternalEdgeIndex(PropertyKey key, String backingIndex) {
        createExternalIndex(key,Edge.class,backingIndex);
    }

    public TitanGraphIndex getExternalIndex(Class<? extends Element> clazz, String backingIndex) {
        String prefix;
        if (Vertex.class.isAssignableFrom(clazz)) prefix = "v";
        else if (Edge.class.isAssignableFrom(clazz)) prefix = "e";
        else if (TitanVertexProperty.class.isAssignableFrom(clazz)) prefix = "p";
        else throw new AssertionError(clazz.toString());

        String indexName = prefix+backingIndex;
        TitanGraphIndex index = mgmt.getGraphIndex(indexName);
        if (index==null) {
            index = mgmt.buildIndex(indexName,clazz).buildMixedIndex(backingIndex);
        }
        return index;
    }

    private void createExternalIndex(PropertyKey key, Class<? extends Element> clazz, String backingIndex) {
        mgmt.addIndexKey(getExternalIndex(clazz,backingIndex),key);
    }

    public PropertyKey makeKey(String name, Class datatype) {
        PropertyKey key = mgmt.makePropertyKey(name).dataType(datatype).cardinality(Cardinality.SINGLE).make();
        return key;
    }

    public EdgeLabel makeLabel(String name) {
        return mgmt.makeEdgeLabel(name).make();
    }

    public EdgeLabel makeKeyedEdgeLabel(String name, PropertyKey sort, PropertyKey signature) {
        EdgeLabel relType = ((StandardEdgeLabelMaker)tx.makeEdgeLabel(name)).
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

    public TitanVertex getVertex(String key, Object value) {
        return getVertex(tx,key,value);
    }

    public TitanVertex getVertex(PropertyKey key, Object value) {
        return getVertex(tx,key,value);
    }

    public static TitanVertex getVertex(TitanTransaction tx, String key, Object value) {
        return (TitanVertex)getOnlyElement(tx.query().has(key,value).vertices(),null);
    }

    public static TitanVertex getVertex(TitanTransaction tx, PropertyKey key, Object value) {
        return getVertex(tx, key.name(), value);
    }

    public static double round(double d) {
        return Math.round(d*1000.0)/1000.0;
    }

    public static TitanVertex getOnlyVertex(TitanGraphQuery<?> query) {
        return (TitanVertex)getOnlyElement(query.vertices());
    }

    public static TitanEdge getOnlyEdge(TitanVertexQuery<?> query) {
        return (TitanEdge)getOnlyElement(query.edges());
    }

    public static<E> E getOnlyElement(Iterable<E> traversal) {
        return getOnlyElement(traversal.iterator());
    }

    public static<E> E getOnlyElement(Iterator<E> traversal) {
        if (!traversal.hasNext()) throw new NoSuchElementException();
        return getOnlyElement(traversal,null);
    }

    public static<E> E getOnlyElement(Iterable<E> traversal, E defaultElement) {
        return getOnlyElement(traversal.iterator(),defaultElement);
    }

    public static<E> E getOnlyElement(Iterator<E> traversal, E defaultElement) {
        if (!traversal.hasNext()) return defaultElement;
        E result = traversal.next();
        if (traversal.hasNext()) throw new IllegalArgumentException("Traversal contains more than 1 element: " + result + ", " + traversal.next());
        return result;
    }

//    public static<E> E getOnlyElement(GraphTraversal<?,E> traversal) {
//        if (!traversal.hasNext()) throw new NoSuchElementException();
//        return getOnlyElement(traversal,null);
//    }
//
//    public static<E> E getOnlyElement(GraphTraversal<?,E> traversal, E defaultElement) {
//        if (!traversal.hasNext()) return defaultElement;
//        E result = traversal.next();
//        if (traversal.hasNext()) throw new IllegalArgumentException("Traversal contains more than 1 element: " + result + ", " + traversal.next());
//        return result;
//    }

    public static void assertMissing(TitanGraphTransaction g, Object vid) {
        assertFalse(g.vertices(vid).hasNext());
    }

    public static TitanVertex getV(TitanGraphTransaction g, Object vid) {
        if (!g.vertices(vid).hasNext()) return null;
        return (TitanVertex)g.vertices(vid).next();
    }

    public static TitanEdge getE(TitanGraphTransaction g, Object eid) {
        if (!g.edges(eid).hasNext()) return null;
        return (TitanEdge)g.edges(eid).next();
    }

    public static String n(Object obj) {
        if (obj instanceof RelationType) return ((RelationType)obj).name();
        else return obj.toString();
    }

    public static long getId(Element e) {
        return ((TitanElement)e).longId();
    }

    public static void verifyElementOrder(Iterable<? extends Element> elements, String key, Order order, int expectedCount) {
        verifyElementOrder(elements.iterator(), key, order, expectedCount);
    }

    public static void verifyElementOrder(Iterator<? extends Element> elements, String key, Order order, int expectedCount) {
        Comparable previous = null;
        int count = 0;
        while (elements.hasNext()) {
            Element element = elements.next();
            Comparable current = element.value(key);
            if (previous != null) {
                int cmp = previous.compareTo(current);
                assertTrue(previous + " <> " + current + " @ " + count,
                        order == Order.ASC ? cmp <= 0 : cmp >= 0);
            }
            previous = current;
            count++;
        }
        assertEquals(expectedCount, count);
    }

    public static <T> Stream<T> asStream(final Iterator<T> source) {
        final Iterable<T> iterable = () -> source;
        return StreamSupport.stream(iterable.spliterator(),false);
    }

}
