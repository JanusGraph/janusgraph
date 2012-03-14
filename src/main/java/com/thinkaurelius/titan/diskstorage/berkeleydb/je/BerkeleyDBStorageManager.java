package com.thinkaurelius.titan.diskstorage.berkeleydb.je;


import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyValueStore;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class BerkeleyDBStorageManager implements KeyValueStorageManager {

	private final Map<String,BerkeleyKeyValueStore> stores;
	
	private Environment environment;
	private final File directory;
	private final boolean transactional;
	private final boolean isReadOnly;
	private final boolean batchLoading;
	
	public BerkeleyDBStorageManager(File directory, boolean readOnly, boolean transactional, boolean batchLoading) {
		stores = new HashMap<String, BerkeleyKeyValueStore>();
		this.transactional = transactional;
		this.directory=directory;
		this.isReadOnly= readOnly;
		this.batchLoading=batchLoading;
	}

	public void initialize(int cachePercent) throws GraphStorageException {
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
			throw new GraphStorageException(e);
		}
		
	}
	
	@Override
	public TransactionHandle beginTransaction() {
		try {
			Transaction tx = null;
			if (transactional) {
				tx = environment.beginTransaction(null, null);
			}
			return new BDBTxHandle(tx);
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
	}

	
	@Override
	public KeyValueStore openDatabase(String name) {
		return openOrderedDatabase(name);
	}


	@Override
	public OrderedKeyValueStore openOrderedDatabase(String name) throws GraphStorageException {
		if (stores.containsKey(name)) {
			BerkeleyKeyValueStore store = stores.get(name);
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
			BerkeleyKeyValueStore store =  new BerkeleyKeyValueStore(name,db,this);
			stores.put(name, store);
			return store;
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
	}
	
	
	void removeDatabase(BerkeleyKeyValueStore db) {
		if (!stores.containsKey(db.getName())) {
			throw new GraphStorageException("Tried to remove an unkown database from the storage manager");
		}
		stores.remove(db.getName());
	}


	@Override
	public void close() throws GraphStorageException {
		if (environment!=null) {
			if (!stores.isEmpty()) throw new GraphStorageException("Cannot close manager since some databases are still open");
			try {
				environment.close();
			} catch (DatabaseException e) {
				throw new GraphStorageException(e);
			}
		}
		
	}




}
