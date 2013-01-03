package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.TransactionalIDManager;
import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockTransaction;
import com.thinkaurelius.titan.diskstorage.locking.transactional.TransactionalLockStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Backend {

    private final Logger log = LoggerFactory.getLogger(Backend.class);

    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     *
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String VERTEXINDEX_STORE_NAME = "propertyindex";

    public static final String ID_STORE_NAME = "titan_ids";


    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final String STORE_LOCAL = "local";
    public static final String STORE_BERKELEYDB = "berkeleyje";
    public static final String STORE_CASSANDRA = "cassandra";
    public static final String STORE_CASSANDRATHRIFT = "cassandrathrift";
    public static final String STORE_ASTYANAX = "astyanax";
    public static final String STORE_EMBEDDEDCASSANDRA = "embeddedcassandra";
    public static final String STORE_HBASE = "hbase";
    private static final Map<String,Class<? extends StoreManager>> preregisteredStorageManagers =
            new HashMap<String,Class<? extends StoreManager>>() {{
        put(STORE_LOCAL, BerkeleyJEStoreManager.class);
        put(STORE_BERKELEYDB, BerkeleyJEStoreManager.class);
        put(STORE_CASSANDRA, AstyanaxStoreManager.class);
        put(STORE_CASSANDRATHRIFT, CassandraThriftStoreManager.class);
        put(STORE_ASTYANAX, AstyanaxStoreManager.class);
        put(STORE_HBASE, HBaseStoreManager.class);
        put(STORE_EMBEDDEDCASSANDRA, CassandraEmbeddedStoreManager.class);
    }};
    
    public static final Map<String,Integer> STATIC_KEY_LENGTHS = new HashMap<String,Integer>() {{
        put(EDGESTORE_NAME,8);
        put(EDGESTORE_NAME +LOCK_STORE_SUFFIX,8);
        put(ID_STORE_NAME,4);
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

        int bufferSizeTmp = storageConfig.getInt(BUFFER_SIZE_KEY,BUFFER_SIZE_DEFAULT);
        Preconditions.checkArgument(bufferSizeTmp >= 0, "Buffer size must be non-negative (use 0 to disable)");
        if (!storeFeatures.supportsBatchMutation()) {
            bufferSize=0;
            log.debug("Buffering disabled because backend does not support batch mutations");
        } else bufferSize=bufferSizeTmp;

        if (!storeFeatures.supportsLocking() && storeFeatures.supportsConsistentKeyOperations()) {
            lockConfiguration = new ConsistentKeyLockConfiguration(storageConfig,storeManager.toString());
        } else {
            lockConfiguration = null;
        }

        writeAttempts = storageConfig.getInt(WRITE_ATTEMPTS_KEY, WRITE_ATTEMPTS_DEFAULT);
        Preconditions.checkArgument(writeAttempts>0,"Write attempts must be positive");
        readAttempts = storageConfig.getInt(READ_ATTEMPTS_KEY, READ_ATTEMPTS_DEFAULT);
        Preconditions.checkArgument(readAttempts>0,"Read attempts must be positive");
        persistAttemptWaittime = storageConfig.getInt(STORAGE_ATTEMPT_WAITTIME_KEY, STORAGE_ATTEMPT_WAITTIME_DEFAULT);
        Preconditions.checkArgument(persistAttemptWaittime>0,"Persistence attempt retry wait time must be non-negative");

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
                store = new ConsistentKeyLockStore(store,getStore(store.getName()+LOCK_STORE_SUFFIX),lockConfiguration);
            } else throw new IllegalArgumentException("Store needs to support some form of locking");
        }
        return store;
    }
    
    private KeyColumnValueStore getBufferStore(String name) throws StorageException {
        Preconditions.checkArgument(bufferSize<=1 || storeManager.getFeatures().supportsBatchMutation());
        KeyColumnValueStore store = null;
        if (isKeyColumnValueStore) {
            assert storeManager instanceof KeyColumnValueStoreManager;
            store = ((KeyColumnValueStoreManager)storeManager).openDatabase(name);
            if (bufferSize>1) {
                store = new BufferedKeyColumnValueStore(store,true);
            }
        } else {
            assert storeManager instanceof KeyValueStoreManager;
            KeyValueStore kvstore = ((KeyValueStoreManager)storeManager).openDatabase(name);
            if (bufferSize>1) {
                //TODO: support buffer mutations for KeyValueStores
            }
            store = KeyValueStoreManagerAdapter.wrapKeyValueStore(kvstore, STATIC_KEY_LENGTHS);
        }
        return store;
    }

    private KeyColumnValueStore getStore(String name) throws StorageException {
        if (isKeyColumnValueStore) {
            return ((KeyColumnValueStoreManager)storeManager).openDatabase(name);
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
                idAuthority = new TransactionalIDManager(idStore,storeManager,config);
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                idAuthority = new ConsistentKeyIDManager(idStore,storeManager,config);
            } else {
                throw new IllegalStateException("Store needs to support consistent key or transactional operations for ID manager to guarantee proper id allocations");
            }
            edgeStore = getLockStore(getBufferStore(EDGESTORE_NAME));
            vertexIndexStore = getLockStore(getBufferStore(VERTEXINDEX_STORE_NAME));

            if (hashPrefixIndex) vertexIndexStore = new HashPrefixKeyColumnValueStore(vertexIndexStore,4);
        } catch (StorageException e) {
            throw new TitanException("Could not initialize backend",e);
        }
    }

    public final static StoreManager getStorageManager(Configuration storageConfig) {
        String clazzname = storageConfig.getString(
                GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,GraphDatabaseConfiguration.STORAGE_BACKEND_DEFAULT);
        if (preregisteredStorageManagers.containsKey(clazzname.toLowerCase())) {
            clazzname = preregisteredStorageManagers.get(clazzname.toLowerCase()).getCanonicalName();
        }

        try {
            Class clazz = Class.forName(clazzname);
            Constructor constructor = clazz.getConstructor(Configuration.class);
            StoreManager storage = (StoreManager)constructor.newInstance(storageConfig);
            return storage;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find storage manager class" + clazzname);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Configured storage manager does not have required constructor: " + clazzname);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname,e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname,e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname,e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname,e);
        }
    }

    //1. Store

    public KeyColumnValueStore getEdgeStore() {
        Preconditions.checkNotNull(edgeStore,"Backend has not yet been initialized");
        return edgeStore;
    }

    public KeyColumnValueStore getVertexIndexStore() {
        Preconditions.checkNotNull(vertexIndexStore,"Backend has not yet been initialized");
        return vertexIndexStore;
    }

    public IDAuthority getIDAuthority() {
        Preconditions.checkNotNull(idAuthority,"Backend has not yet been initialized");
        return idAuthority;
    }

    public StoreFeatures getStoreFeatures() {
        return storeManager.getFeatures();
    }

    //2. Entity Index

    //3. Messaging queues

    public BackendTransaction beginTransaction() throws StorageException {
        StoreTransaction tx = storeManager.beginTransaction(ConsistencyLevel.DEFAULT);
        if (bufferSize>1) {
            assert storeManager.getFeatures().supportsBatchMutation();
            if (isKeyColumnValueStore) {
                assert storeManager instanceof KeyColumnValueStoreManager;
                tx = new BufferTransaction(tx,(KeyColumnValueStoreManager)storeManager,bufferSize,writeAttempts,persistAttemptWaittime);
            } else {
                assert storeManager instanceof KeyValueStoreManager;
                //TODO: support buffer mutations
            }
        }
        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTransactions()) {
                //No transaction wrapping needed
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                tx = new ConsistentKeyLockTransaction(tx,storeManager.beginTransaction(ConsistencyLevel.KEY_CONSISTENT));
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

    public String getLastSeenTitanVersion() throws StorageException {
        return storeManager.getLastSeenTitanVersion();
    }

    public void setTitanVersionToLatest() throws StorageException {
        storeManager.setTitanVersionToLatest();
    }
}
