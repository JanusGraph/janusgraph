package com.thinkaurelius.titan.diskstorage.berkeleydb.api;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.Transaction;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyValueStore;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

public class BerkeleyDBStorageManager implements KeyValueStorageManager {

	private final Map<String,BerkeleyKeyValueStore> stores;
	
	private Environment environment;
	private final File directory;
	private final boolean transactional;
	private final boolean isPrivate;
	private final boolean isReadOnly;
	
	public BerkeleyDBStorageManager(File directory, boolean readOnly, boolean transactional, boolean isPrivate) {
		stores = new HashMap<String, BerkeleyKeyValueStore>();
		this.transactional = transactional;
		this.directory=directory;
		this.isPrivate = isPrivate;
		this.isReadOnly= readOnly;
	}

	public void initialize(long cacheSize) throws GraphStorageException {
		try {
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(transactional);
			envConfig.setInitializeCache(true);
			envConfig.setPrivate(isPrivate);
			envConfig.setCacheSize(cacheSize);

			//Open the environment
			environment = new Environment(directory, envConfig);
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		} catch (FileNotFoundException e) {
			throw new GraphStorageException(e);
		}
		
	}
	
	private String getFileName(String dbname) {
		return directory.getAbsolutePath() + File.separator + dbname;
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
		if (stores.containsKey(name)) return stores.get(name);
		try {
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setReadOnly(isReadOnly);
			dbConfig.setAllowCreate(true);
			dbConfig.setTransactional(transactional);			
			dbConfig.setType(DatabaseType.HASH);
			dbConfig.setByteOrder(4321);
			Database db = environment.openDatabase(null, getFileName(name), name, dbConfig);
			BerkeleyKeyValueStore store =  new BerkeleyKeyValueStore(name,db,this);
			stores.put(name, store);
			return store;
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		} catch (FileNotFoundException e2) {
			throw new GraphStorageException(e2);
		}
	}


	@Override
	public OrderedKeyValueStore openOrderedDatabase(String name) throws GraphStorageException {
		if (stores.containsKey(name)) {
			BerkeleyKeyValueStore store = stores.get(name);
			if (store.getConfiguration().getType()!=DatabaseType.BTREE) {
				throw new GraphStorageException("Trying to retrieve a unordered database as an ordered one!");
			}
			return store;
		}
		try {
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setReadOnly(isReadOnly);
			dbConfig.setAllowCreate(true);
			dbConfig.setTransactional(transactional);
			dbConfig.setByteOrder(4321);
			dbConfig.setType(DatabaseType.BTREE);
			Database db = environment.openDatabase(null, getFileName(name), name, dbConfig);
			BerkeleyKeyValueStore store =  new BerkeleyKeyValueStore(name,db,this);
			stores.put(name, store);
			return store;
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		} catch (FileNotFoundException e2) {
			throw new GraphStorageException(e2);
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
