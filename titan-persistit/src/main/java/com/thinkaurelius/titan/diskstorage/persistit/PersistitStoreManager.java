package com.thinkaurelius.titan.diskstorage.persistit;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.persistit.*;
import com.persistit.exception.PersistitException;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;


import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @todo: confirm that the initial sessions created on store startup are not hanging around forever
 *
 */
public class PersistitStoreManager implements OrderedKeyValueStoreManager {

    private final Map<String, PersistitKeyValueStore> stores;
    private static final StoreFeatures features = new StoreFeatures();
    final static String VOLUME_NAME = "titan";
    final static String BUFFER_COUNT_KEY = "buffercount";
    final static Integer BUFFER_COUNT_DEFAULT = 5000;

    static {
        features.supportsTransactions = true;
        features.isDistributed = false;

        //@todo: figure out what these do, Copied from Berkeley for now
        features.supportsScan = true;
        features.supportsBatchMutation = false;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = true;
        features.isKeyOrdered = true;
        features.hasLocalKeyPartition = false;
    }

    private Persistit db;
    private Exchange exchange;

    private Configuration config;
    private Properties properties;
    private  File directory;

    public PersistitStoreManager(Configuration configuration) throws StorageException {
        stores = new HashMap<String, PersistitKeyValueStore>();

        config = cloneConfig(configuration);
        // read config and setup
        properties = new Properties();
        String datapath = config.getString(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY);
        Integer bufferCount = config.getInt(BUFFER_COUNT_KEY, BUFFER_COUNT_DEFAULT);
        Preconditions.checkArgument(datapath != null, "Need to specify storage directory");
        directory = getOrCreateDataDirectory(datapath);
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
            throw new PermanentStorageException(ex.toString());
        }

        //do some additional config setup
        config.addProperty(Backend.TITAN_BACKEND_VERSION, "0.3.1");
    }

    Volume getVolume() {
        return db.getVolume(VOLUME_NAME);
    }

    @Override
    public PersistitKeyValueStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name)) {
            return stores.get(name);
        }

        PersistitTransaction tx = new PersistitTransaction(db, ConsistencyLevel.DEFAULT);
        PersistitKeyValueStore store = new PersistitKeyValueStore(name, this, db);
        tx.commit();
        stores.put(name, store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    public void removeDatabase(PersistitKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        stores.remove(db.getName());
    }

    private static File getOrCreateDataDirectory(String location) throws StorageException {
        File storageDir = new File(location);

        if (storageDir.exists() && storageDir.isFile())
            throw new PermanentStorageException(String.format("%s exists but is a file.", location));

        if (!storageDir.exists() && !storageDir.mkdirs())
            throw new PermanentStorageException(String.format("Failed to create directory %s for BerkleyDB storage.", location));

        return storageDir;
    }

    private static Configuration cloneConfig(Configuration src) {
        BaseConfiguration dst = new BaseConfiguration();
        Iterator<String> keys = src.getKeys();
        while (keys.hasNext()) {
            String k = keys.next();
            dst.addProperty(k, src.getProperty(k));
        }
        return dst;
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
                throw new PermanentStorageException(ex.toString());
            }
        }
    }

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Handle
     */
    @Override
    public PersistitTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        //all Exchanges created by a thread share the same transaction context
        return new PersistitTransaction(db, level);
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void clearStorage() throws StorageException {
        for(String key : stores.keySet()) {
            PersistitKeyValueStore store = stores.remove(key);
            store.clear();
        }

        Volume volume;
        String[] treeNames;
        try {
            volume = db.getVolume(VOLUME_NAME);
            treeNames = volume.getTreeNames();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());

        }

        for (String treeName : treeNames) {
            try {
                Exchange ex = new Exchange(db, volume, treeName, false);
                ex.removeTree();
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex.toString());
            }

        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        return config.getString(key);
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        //@todo: this
        throw new PermanentStorageException("write this part");
    }
}
