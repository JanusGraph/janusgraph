package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.TransactionalIDManager;
import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.BufferTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.BufferedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockTransaction;
import com.thinkaurelius.titan.diskstorage.locking.transactional.TransactionalLockStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Backend {

    private static final Logger log = LoggerFactory.getLogger(Backend.class);

    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     * <p/>
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String VERTEXINDEX_STORE_NAME = "propertyindex";

    public static final String ID_STORE_NAME = "titan_ids";

    public static final String TITAN_BACKEND_VERSION = "titan-version";


    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final Map<String, Integer> STATIC_KEY_LENGTHS = new HashMap<String, Integer>() {{
        put(EDGESTORE_NAME, 8);
        put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, 8);
        put(ID_STORE_NAME, 4);
    }};

    private final StoreManager storeManager;
    private final boolean isKeyColumnValueStore;
    private final StoreFeatures storeFeatures;

    private KeyColumnValueStore edgeStore;
    private KeyColumnValueStore vertexIndexStore;
    private IDAuthority idAuthority;

    private final ConsistentKeyLockConfiguration lockConfiguration;
    private final int bufferSize;
    private final boolean hashPrefixIndex;

    private final int writeAttempts;
    private final int readAttempts;
    private final int persistAttemptWaittime;

    public Backend(Configuration storageConfig) {
        storeManager = getStorageManager(storageConfig);
        isKeyColumnValueStore = storeManager instanceof KeyColumnValueStoreManager;
        storeFeatures = storeManager.getFeatures();

        int bufferSizeTmp = storageConfig.getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
        Preconditions.checkArgument(bufferSizeTmp >= 0, "Buffer size must be non-negative (use 0 to disable)");
        if (!storeFeatures.supportsBatchMutation()) {
            bufferSize = 0;
            log.debug("Buffering disabled because backend does not support batch mutations");
        } else bufferSize = bufferSizeTmp;

        if (!storeFeatures.supportsLocking() && storeFeatures.supportsConsistentKeyOperations()) {
            lockConfiguration = new ConsistentKeyLockConfiguration(storageConfig, storeManager.toString());
        } else {
            lockConfiguration = null;
        }

        writeAttempts = storageConfig.getInt(WRITE_ATTEMPTS_KEY, WRITE_ATTEMPTS_DEFAULT);
        Preconditions.checkArgument(writeAttempts > 0, "Write attempts must be positive");
        readAttempts = storageConfig.getInt(READ_ATTEMPTS_KEY, READ_ATTEMPTS_DEFAULT);
        Preconditions.checkArgument(readAttempts > 0, "Read attempts must be positive");
        persistAttemptWaittime = storageConfig.getInt(STORAGE_ATTEMPT_WAITTIME_KEY, STORAGE_ATTEMPT_WAITTIME_DEFAULT);
        Preconditions.checkArgument(persistAttemptWaittime > 0, "Persistence attempt retry wait time must be non-negative");

        if (storeFeatures.isDistributed() && storeFeatures.isKeyOrdered()) {
            log.debug("Wrapping index store with HashPrefix");
            hashPrefixIndex = true;
        } else {
            hashPrefixIndex = false;
        }
    }


    private KeyColumnValueStore getLockStore(KeyColumnValueStore store) throws StorageException {
        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTransactions()) {
                store = new TransactionalLockStore(store);
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                store = new ConsistentKeyLockStore(store, getStore(store.getName() + LOCK_STORE_SUFFIX), lockConfiguration);
            } else throw new IllegalArgumentException("Store needs to support some form of locking");
        }
        return store;
    }

    private KeyColumnValueStore getBufferStore(String name) throws StorageException {
        Preconditions.checkArgument(bufferSize <= 1 || storeManager.getFeatures().supportsBatchMutation());
        KeyColumnValueStore store = null;
        if (isKeyColumnValueStore) {
            assert storeManager instanceof KeyColumnValueStoreManager;
            store = ((KeyColumnValueStoreManager) storeManager).openDatabase(name);
            if (bufferSize > 1) {
                store = new BufferedKeyColumnValueStore(store, true);
            }
        } else {
            assert storeManager instanceof KeyValueStoreManager;
            KeyValueStore kvstore = ((KeyValueStoreManager) storeManager).openDatabase(name);
            if (bufferSize > 1) {
                //TODO: support buffer mutations for KeyValueStores
            }
            store = KeyValueStoreManagerAdapter.wrapKeyValueStore(kvstore, STATIC_KEY_LENGTHS);
        }
        return store;
    }

    private KeyColumnValueStore getStore(String name) throws StorageException {
        if (isKeyColumnValueStore) {
            return ((KeyColumnValueStoreManager) storeManager).openDatabase(name);
        } else {
            return KeyValueStoreManagerAdapter.wrapKeyValueStore(
                    ((KeyValueStoreManager) storeManager).openDatabase(name), STATIC_KEY_LENGTHS);
        }
    }


    public void initialize(Configuration config) {
        try {
            //EdgeStore & VertexIndexStore
            KeyColumnValueStore idStore = getStore(ID_STORE_NAME);
            idAuthority = null;
            if (storeFeatures.supportsTransactions()) {
                idAuthority = new TransactionalIDManager(idStore, storeManager, config);
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                idAuthority = new ConsistentKeyIDManager(idStore, storeManager, config);
            } else {
                throw new IllegalStateException("Store needs to support consistent key or transactional operations for ID manager to guarantee proper id allocations");
            }
            edgeStore = getLockStore(getBufferStore(EDGESTORE_NAME));
            vertexIndexStore = getLockStore(getBufferStore(VERTEXINDEX_STORE_NAME));

            if (hashPrefixIndex) vertexIndexStore = new HashPrefixKeyColumnValueStore(vertexIndexStore, 4);

            String version = storeManager.getConfigurationProperty(TITAN_BACKEND_VERSION);
            if (!TitanConstants.VERSION.equals(version)) {
                if (version == null ||
                        (TitanConstants.COMPATIBLE_VERSIONS.contains(version))) {
                    storeManager.setConfigurationProperty(TITAN_BACKEND_VERSION, TitanConstants.VERSION);
                } else {
                    throw new TitanException("StorageBackend is incompatible with Titan version: " + TitanConstants.VERSION + " vs. " + version);
                }
            }
        } catch (StorageException e) {
            throw new TitanException("Could not initialize backend", e);
        }
    }

    public final static StoreManager getStorageManager(Configuration storageConfig) {
        String clazzname = storageConfig.getString(
                GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, GraphDatabaseConfiguration.STORAGE_BACKEND_DEFAULT);
        if (REGISTERED_STORAGE_MANAGERS.containsKey(clazzname.toLowerCase())) {
            clazzname = REGISTERED_STORAGE_MANAGERS.get(clazzname.toLowerCase());
        }

        try {
            Class clazz = Class.forName(clazzname);
            Constructor constructor = clazz.getConstructor(Configuration.class);
            StoreManager storage = (StoreManager) constructor.newInstance(storageConfig);
            return storage;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find storage manager class: " + clazzname);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Configured storage manager does not have required constructor: " + clazzname);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class: " + clazzname, e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class: " + clazzname, e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class: " + clazzname, e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class: " + clazzname, e);
        }
    }

    //1. Store

    public KeyColumnValueStore getEdgeStore() {
        Preconditions.checkNotNull(edgeStore, "Backend has not yet been initialized");
        return edgeStore;
    }

    public KeyColumnValueStore getVertexIndexStore() {
        Preconditions.checkNotNull(vertexIndexStore, "Backend has not yet been initialized");
        return vertexIndexStore;
    }

    public IDAuthority getIDAuthority() {
        Preconditions.checkNotNull(idAuthority, "Backend has not yet been initialized");
        return idAuthority;
    }

    public StoreFeatures getStoreFeatures() {
        return storeManager.getFeatures();
    }

    //2. Entity Index

    //3. Messaging queues

    public BackendTransaction beginTransaction() throws StorageException {
        StoreTransaction tx = storeManager.beginTransaction(ConsistencyLevel.DEFAULT);
        if (bufferSize > 1) {
            assert storeManager.getFeatures().supportsBatchMutation();
            if (isKeyColumnValueStore) {
                assert storeManager instanceof KeyColumnValueStoreManager;
                tx = new BufferTransaction(tx, (KeyColumnValueStoreManager) storeManager, bufferSize, writeAttempts, persistAttemptWaittime);
            } else {
                assert storeManager instanceof KeyValueStoreManager;
                //TODO: support buffer mutations
            }
        }
        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTransactions()) {
                //No transaction wrapping needed
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                tx = new ConsistentKeyLockTransaction(tx, storeManager.beginTransaction(ConsistencyLevel.KEY_CONSISTENT));
            }
        }
        return new BackendTransaction(tx);
    }

    public void close() throws StorageException {
        edgeStore.close();
        vertexIndexStore.close();
        idAuthority.close();
        storeManager.close();
    }

    public void clearStorage() throws StorageException {
        edgeStore.close();
        vertexIndexStore.close();
        idAuthority.close();
        storeManager.clearStorage();
    }
    
    //############ Registered Storage Managers ##############

    private static final Map<String, String> REGISTERED_STORAGE_MANAGERS = new HashMap<String, String>() {{
        put("local", "com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager");
        put("berkeleyje", "com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager");
        put("persistit", "com.thinkaurelius.titan.diskstorage.persistit.PersistitStoreManager");
        put("cassandra", "com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager");
        put("cassandrathrift", "com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager");
        put("astyanax", "com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager");
        put("hbase", "com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager");
        put("embeddedcassandra", "com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager");
    }};

    static {
        Properties props;

        try {
            props = new Properties();
            InputStream in = TitanFactory.class.getClassLoader().getResourceAsStream(TitanConstants.TITAN_PROPERTIES_FILE);
            if (in!=null && in.available()>0) {
                props.load(in);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        String prefix = "storage.";
        for (String key : props.stringPropertyNames()) {
            if (key.toLowerCase().startsWith(prefix)) {
                String shorthand = key.substring(prefix.length()).toLowerCase();
                String clazz = props.getProperty(key);
                REGISTERED_STORAGE_MANAGERS.put(shorthand,clazz);
                log.debug("Registering shorthand {} for: {}",shorthand,clazz);
            }
        }
    }
//
//    public synchronized static final void registerStorageManager(String name, Class<? extends StoreManager> clazz) {
//        Preconditions.checkNotNull(name);
//        Preconditions.checkNotNull(clazz);
//        Preconditions.checkArgument(!StringUtils.isEmpty(name));
//        Preconditions.checkNotNull(!REGISTERED_STORAGE_MANAGERS.containsKey(name),"A storage manager has already been registered for name: " + name);
//        REGISTERED_STORAGE_MANAGERS.put(name,clazz);
//    }
//
//    public synchronized static final void removeStorageManager(String name) {
//        Preconditions.checkNotNull(name);
//        REGISTERED_STORAGE_MANAGERS.remove(name);
//    }
    
}
