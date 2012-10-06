package com.thinkaurelius.titan.diskstorage.berkeleydb.je;


import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.StorageFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import com.thinkaurelius.titan.diskstorage.util.*;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.idassigner.DefaultIDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class BerkeleyJEStoreManager implements KeyValueStoreManager {

    private final Logger log = LoggerFactory.getLogger(BerkeleyJEStoreManager.class);


    public static final String CACHE_KEY = "cache_percentage";
    public static final int CACHE_DEFAULT = 65;
    
    private static final String IDMANAGER_KEY = "idmanager_table";
    private static final String IDMANAGER_DEFAULT = "titan_idmanager";
    
	private final Map<String,BerkeleyJEKeyValueStore> stores;
	

	private Environment environment;
	private final File directory;
	private final boolean transactional;
	private final boolean isReadOnly;
	private final boolean batchLoading;
    private final StorageFeatures features;

    private IDBlockSizer blockSizer;
    private volatile boolean hasActiveIDAcquisition;
    private final String idManagerTableName;
    private final ReentrantLock idAcquisitionLock = new ReentrantLock();

	public BerkeleyJEStoreManager(Configuration configuration) throws StorageException {
		stores = new HashMap<String, BerkeleyJEKeyValueStore>();
		directory=new File(configuration.getString(STORAGE_DIRECTORY_KEY));
        Preconditions.checkArgument(directory.isDirectory() && directory.canWrite(),"Cannot open or write to directory: " + directory);
		isReadOnly= configuration.getBoolean(STORAGE_READONLY_KEY,STORAGE_READONLY_DEFAULT);
		batchLoading=configuration.getBoolean(STORAGE_BATCH_KEY,STORAGE_BATCH_DEFAULT);
        boolean transactional = configuration.getBoolean(STORAGE_TRANSACTIONAL_KEY,STORAGE_TRANSACTIONAL_DEFAULT);
        this.blockSizer = new DefaultIDBlockSizer(configuration.getLong(IDAUTHORITY_BLOCK_SIZE_KEY,IDAUTHORITY_BLOCK_SIZE_DEFAULT));
        this.hasActiveIDAcquisition=false;
        if (batchLoading) {
            if (transactional) log.warn("Disabling transactional since batch loading is enabled!");
            transactional=false;
        }
        this.transactional=transactional;
        int cachePercentage = configuration.getInt(CACHE_KEY,CACHE_DEFAULT);
        
        idManagerTableName = configuration.getString(IDMANAGER_KEY,IDMANAGER_DEFAULT);
        initialize(cachePercentage);

        this.features = new StoreFeatures(true,transactional);
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
    public StorageFeatures getFeatures() {
        return features;
    }
    
	@Override
	public BerkeleyJETxHandle beginTransaction() throws StorageException  {
		try {
			Transaction tx = null;
			if (transactional) {
				tx = environment.beginTransaction(null, null);
			}
			return new BerkeleyJETxHandle(tx);
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


    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        idAcquisitionLock.lock();
        try {
            if (hasActiveIDAcquisition) throw new IllegalStateException("IDBlockSizer cannot be changed after IDs have already been assigned");
            this.blockSizer=sizer;
        } finally {
            idAcquisitionLock.unlock();
        }
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {

            long blockSize = blockSizer.getBlockSize(partition);
            Preconditions.checkArgument(blockSize<Integer.MAX_VALUE);
            int counter = 1;
            BerkeleyJETxHandle tx = null;
            try {
                tx = beginTransaction();
                ByteBuffer value = idDB.get(key,tx);
                if (value!=null) {
                    assert value.remaining()==4;
                    counter = value.getInt();
                }
                Preconditions.checkArgument(Integer.MAX_VALUE-blockSize>counter);
                int next = counter + (int)blockSize;
                idDB.insert(new KeyValueEntry(key,ByteBufferUtil.getIntByteBuffer(next)),tx.getTransaction(),true);
                tx.commit();
                return new long[]{counter,next};
            } catch (RuntimeException e) {
                if (tx!=null) tx.abort();
                throw e;
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
            BerkeleyJEKeyValueStore idmanager = stores.get(idManagerTableName);
            if (idmanager!=null) idmanager.close();
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


}
