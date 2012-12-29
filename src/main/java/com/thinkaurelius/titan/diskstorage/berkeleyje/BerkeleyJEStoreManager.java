package com.thinkaurelius.titan.diskstorage.berkeleyje;


import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.thinkaurelius.titan.core.Constants;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class BerkeleyJEStoreManager implements KeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEStoreManager.class);

    private static final String VERSION_FILE_NAME = "last-titan-version.dje";

    public static final String CACHE_KEY = "cache-percentage";
    public static final int CACHE_DEFAULT = 65;

	private final Map<String,BerkeleyJEKeyValueStore> stores;
	

	private Environment environment;
	private final File directory;
	private final boolean transactional;
	private final boolean isReadOnly;
	private final boolean batchLoading;
    private final StoreFeatures features;

	public BerkeleyJEStoreManager(Configuration configuration) throws StorageException {
		stores = new HashMap<String, BerkeleyJEKeyValueStore>();
        String storageDir = configuration.getString(STORAGE_DIRECTORY_KEY);
        Preconditions.checkArgument(storageDir!=null,"Need to specify storage directory");
		directory=new File(storageDir);
        Preconditions.checkArgument(directory.isDirectory() && directory.canWrite(),"Cannot open or write to directory: " + directory);
		isReadOnly= configuration.getBoolean(STORAGE_READONLY_KEY,STORAGE_READONLY_DEFAULT);
		batchLoading=configuration.getBoolean(STORAGE_BATCH_KEY,STORAGE_BATCH_DEFAULT);
        boolean transactional = configuration.getBoolean(STORAGE_TRANSACTIONAL_KEY,STORAGE_TRANSACTIONAL_DEFAULT);
        if (batchLoading) {
            if (transactional) log.warn("Disabling transactional since batch loading is enabled!");
            transactional=false;
        }
        this.transactional=transactional;
        if (!transactional) log.warn("Transactions are disabled. Ensure that there is at most one Titan instance interacting with this BerkeleyDB instance, otherwise your database may corrupt.");
        int cachePercentage = configuration.getInt(CACHE_KEY,CACHE_DEFAULT);

        /* If directory was created by this run it's safe to create version file, flag set by GDC.getConfiguration(File) */
        if (!configuration.getBoolean(GraphDatabaseConfiguration.EXISTING_DIRECTORY_KEY)) {
            createVersionFile(getVersionFile(directory));
        }

        initialize(cachePercentage);

        features = new StoreFeatures();
        features.supportsScan=true; features.supportsBatchMutation=false; features.supportsTransactions=true;
        features.supportsConsistentKeyOperations=false; features.supportsLocking=true; features.isKeyOrdered=true;
        features.isDistributed=false; features.hasLocalKeyPartition=false;
	}

	private void initialize(int cachePercent) throws StorageException {
		try {
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(transactional);
			envConfig.setCachePercent(cachePercent);
			
			if (batchLoading) {
				envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
				envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
			}
			
			//Open the environment
			environment = new Environment(directory, envConfig);
		} catch (DatabaseException e) {
			throw new PermanentStorageException("Error during BerkeleyJE initialization: ",e);
		}
		
	}
	
    @Override
    public StoreFeatures getFeatures() {
        return features;
    }
    
	@Override
	public BerkeleyJETx beginTransaction(ConsistencyLevel level) throws StorageException  {
		try {
			Transaction tx = null;
			if (transactional) {
				tx = environment.beginTransaction(null, null);
			}
			return new BerkeleyJETx(tx,level);
		} catch (DatabaseException e) {
			throw new PermanentStorageException("Could not start BerkeleyJE transaction",e);
		}
	}


	@Override
	public BerkeleyJEKeyValueStore openDatabase(String name) throws StorageException {
		Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
			BerkeleyJEKeyValueStore store = stores.get(name);
			return store;
		}
		try {
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setReadOnly(isReadOnly);
			dbConfig.setAllowCreate(true);
			dbConfig.setTransactional(transactional);

			dbConfig.setKeyPrefixing(true);
			
			if (batchLoading) {
				dbConfig.setDeferredWrite(true);
			}
			
			Database db = environment.openDatabase(null, name, dbConfig);
			BerkeleyJEKeyValueStore store =  new BerkeleyJEKeyValueStore(name,db,this);
			stores.put(name, store);
			return store;
		} catch (DatabaseException e) {
			throw new PermanentStorageException("Could not open BerkeleyJE data store",e);
		}
	}

    void removeDatabase(BerkeleyJEKeyValueStore db) {
		if (!stores.containsKey(db.getName())) {
			throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
		}
		stores.remove(db.getName());
	}


	@Override
	public void close() throws StorageException {
		if (environment!=null) {
			if (!stores.isEmpty()) throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
			try {
				environment.close();
			} catch (DatabaseException e) {
				throw new PermanentStorageException("Could not close BerkeleyJE database",e);
			}
		}
		
	}

    @Override
    public void clearStorage() throws StorageException  {
        if (!stores.isEmpty()) throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet().toString());

        Transaction tx = null;
        for (String db : environment.getDatabaseNames()) {
            environment.removeDatabase(tx,db);
        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public String getLastSeenTitanVersion() throws StorageException {
        File versionFile = getVersionFile(directory);

        if (!versionFile.exists())
            return null; // most certainly created by Titan < 0.3.0

        DataInputStream version = null;

        try {
            version = new DataInputStream(new FileInputStream(versionFile));
            return version.readUTF();
        } catch (IOException e) {
            throw new PermanentStorageException("Corrupted version file: " + versionFile.getAbsolutePath(), e);
        } finally {
            IOUtils.closeQuietly(version);
        }
    }

    @Override
    public void setTitanVersionToLatest() throws StorageException {
        File versionFile = getVersionFile(directory);

        if (versionFile.exists())
            versionFile.delete(); // just delete the old one, saves us code

        createVersionFile(versionFile);
    }

    private static void createVersionFile(File versionFile) throws StorageException {
        Preconditions.checkArgument(!versionFile.exists());

        DataOutputStream s = null;

        try {
            s = new DataOutputStream(new FileOutputStream(versionFile));
            s.writeUTF(Constants.VERSION);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        } finally {
            IOUtils.closeQuietly(s);
        }
    }

    private static File getVersionFile(File dbDirectory) {
        return new File(dbDirectory.getAbsolutePath() + File.separator + VERSION_FILE_NAME);
    }
}
