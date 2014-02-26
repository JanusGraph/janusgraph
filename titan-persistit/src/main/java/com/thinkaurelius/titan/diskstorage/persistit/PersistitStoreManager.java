package com.thinkaurelius.titan.diskstorage.persistit;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;

/**
 * @todo: confirm that the initial sessions created on store startup are not hanging around forever
 */
public class PersistitStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private final Map<String, PersistitKeyValueStore> stores;

    final static String VOLUME_NAME = "titan";

    public static final ConfigOption<Integer> BUFFER_COUNT = new ConfigOption<Integer>(GraphDatabaseConfiguration.STORAGE_NS,"buffercount",
            "Buffer size for Persistit",
            ConfigOption.Type.MASKABLE, 5000, ConfigOption.positiveInt());

//    final static String BUFFER_COUNT_KEY = "buffercount";
//    final static Integer BUFFER_COUNT_DEFAULT = 5000;

    private final Persistit db;
    private final Properties properties;
    protected final StoreFeatures features = getDefaultFeatures();


    public PersistitStoreManager(Configuration configuration) throws StorageException {
        super(configuration);

        stores = new HashMap<String, PersistitKeyValueStore>();

        // read config and setup
        String datapath = configuration.get(GraphDatabaseConfiguration.STORAGE_DIRECTORY);
        Integer bufferCount = configuration.get(BUFFER_COUNT);

        properties = new Properties();
        properties.put("datapath", datapath);

        // On pathSeparator is ":" on 'Nix systems - File.separator is what is intended.

        properties.put("journalpath", directory + File.separator + VOLUME_NAME);
        properties.put("logfile", directory + File.separator + VOLUME_NAME + ".log");

        // @todo: make these tunable
        properties.put("buffer.count.16384", bufferCount.toString());
        properties.put("volume.1", directory + File.separator + VOLUME_NAME
                + ",create,pageSize:16384,initialPages:1000,extensionPages:1000,maximumPages:1000000");

        try {
            db = new Persistit(properties);
            db.initialize();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex);
        }
    }

    Volume getVolume() {
        return db.getVolume(VOLUME_NAME);
    }

    @Override
    public PersistitKeyValueStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name)) {
            return stores.get(name);
        }

        PersistitTransaction tx = new PersistitTransaction(db, StandardTransactionConfig.of());
        PersistitKeyValueStore store = new PersistitKeyValueStore(name, this, db);
        tx.commit();
        stores.put(name, store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws StorageException {
        for (Map.Entry<String,KVMutation> muts : mutations.entrySet()) {
            PersistitKeyValueStore store = openDatabase(muts.getKey());
            KVMutation mut = muts.getValue();
            if (mut.hasAdditions()) {
                for (KeyValueEntry entry : mut.getAdditions()) store.insert(entry.getKey(),entry.getValue(),txh);
            }
            if (mut.hasDeletions()) {
                for (StaticBuffer del : mut.getDeletions()) store.delete(del,txh);
            }
        }
    }

    public void removeDatabase(PersistitKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        stores.remove(db.getName());
    }

    @Override
    public void close() throws StorageException {
        if (db != null) {
            if (!stores.isEmpty()) {
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            }
            try {
                db.close(true);
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            }
        }
    }

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Handle
     */
    @Override
    public PersistitTransaction beginTransaction(final TransactionHandleConfig config) throws StorageException {
        //all Exchanges created by a thread share the same transaction context
        return new PersistitTransaction(db, config);
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void clearStorage() throws StorageException {
        for (String key : stores.keySet()) {
            PersistitKeyValueStore store = stores.remove(key);
            store.clear();
        }

        Volume volume;
        String[] treeNames;
        try {
            volume = db.getVolume(VOLUME_NAME);
            treeNames = volume.getTreeNames();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex);

        }

        for (String treeName : treeNames) {
            try {
                Exchange ex = new Exchange(db, volume, treeName, false);
                ex.removeTree();
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            }

        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }

    private StoreFeatures getDefaultFeatures() {
        Configuration c = GraphDatabaseConfiguration.buildConfiguration();

        return new StandardStoreFeatures.Builder()
            .transactional(true)
            .orderedScan(true)
            .locking(true)
            .keyOrdered(true)
            .keyConsistent(c)
            .build();

//        StoreFeatures features = new StoreFeatures();
//
//        features.supportsTxIsolation = true;
//        features.isDistributed = false;
//
//        //@todo: figure out what these do, Copied from Berkeley for now
//        features.supportsOrderedScan = true;
//        features.supportsUnorderedScan = false;
//        features.supportsBatchMutation = false;
//        features.supportsConsistentKeyOperations = true;
//        features.supportsLocking = true;
//        features.isKeyOrdered = true;
//        features.hasLocalKeyPartition = false;
//        features.supportsMultiQuery = false;
//
//        return features;
    }
}
