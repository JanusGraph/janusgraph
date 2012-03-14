package com.thinkaurelius.titan.diskstorage.berkeleydb.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.MultipleDataEntry;
import com.sleepycat.db.MultipleKeyDataEntry;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

import com.thinkaurelius.titan.diskstorage.LockType;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyValueStore;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.exceptions.DuplicateEntryException;

public class BerkeleyKeyValueStore implements OrderedKeyValueStore {
	
	private Logger log = LoggerFactory.getLogger(BerkeleyKeyValueStore.class);
	
	private final Database db;
	private final String name;
	private final BerkeleyDBStorageManager manager;
	
	public BerkeleyKeyValueStore(String n, Database data, BerkeleyDBStorageManager m) {
		db = data;
		name = n;
		manager = m;
	}
	
	public DatabaseConfig getConfiguration() {
		try {
			return db.getConfig();
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
	}
	
	public String getName() {
		return name;
	}
	
	private static final Transaction getTransaction(TransactionHandle txh) {
		return (txh==null?null:((BDBTxHandle)txh).getTransaction());
	}
	
	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		Transaction tx = getTransaction(txh);
		try {
			DatabaseEntry dbkey = getDataEntry(key);
			
			OperationStatus status = db.exists(tx, dbkey);
			return status==OperationStatus.SUCCESS;

		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
	}
	
	@Override
	public void close() throws GraphStorageException {	
		try {
			db.close();
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
		manager.removeDatabase(this);
	}


	@Override
	public ByteBuffer get(ByteBuffer key, TransactionHandle txh) {
		Transaction tx = getTransaction(txh);
		try {
			DatabaseEntry dbkey = getDataEntry(key);
			DatabaseEntry data = new DatabaseEntry();
			
			OperationStatus status = db.get(tx, dbkey, data, LockMode.DEFAULT);
			if (status==OperationStatus.SUCCESS) {
				return getByteBuffer(data);
			} else {
				return null;
			}
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
	}
	
	@Override
	public boolean isLocalKey(ByteBuffer key) {
		return true;
	}
	
	@Override
	public void acquireLock(ByteBuffer key, LockType type, TransactionHandle txh) {
		if (getTransaction(txh)==null) {
			throw new GraphStorageException("Enable transaction for locking in BerkeleyDB!");
		} //else we need no locking
	}
	



	@Override
	public boolean containsInInterval(ByteBuffer startKey, ByteBuffer endKey, TransactionHandle txh) {
		Transaction tx = getTransaction(txh);
		Cursor cursor = null;
		boolean result = false;
		try {
			DatabaseEntry foundKey = getDataEntry(startKey);
			DatabaseEntry foundData = new DatabaseEntry();
			
			cursor = db.openCursor(tx, null);
			OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT);
			if (status==OperationStatus.SUCCESS) {
		        ByteBuffer found = getByteBuffer(foundKey);
		        endKey.rewind();
				if (ByteBufferUtil.isSmallerThan(found, endKey)) {
					result= true;
				}
			}
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		} finally {
			try {
			cursor.close();
			} catch (DatabaseException e) {
				throw new GraphStorageException(e);
			}
		}
		return result;
	}

	@Override
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, TransactionHandle txh) {
		return getSlice(keyStart,keyEnd,Integer.MAX_VALUE,txh);
	}
	
	@Override
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, TransactionHandle txh) {
		Transaction tx =getTransaction(txh);
		Cursor cursor = null;
		List<KeyValueEntry> result;
		try {
			DatabaseEntry foundKey = getDataEntry(keyStart);
			DatabaseEntry foundData = new DatabaseEntry();
			
			cursor = db.openCursor(tx, null);
			OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT);
			result = new ArrayList<KeyValueEntry>();
			//Iterate until given condition is satisfied or end of records
			while (status == OperationStatus.SUCCESS) {
				
				ByteBuffer key = getByteBuffer(foundKey);
				keyEnd.rewind();
				if (!ByteBufferUtil.isSmallerThan(key, keyEnd)) break;
				key.rewind();
				
				if (result.size()>=limit) {
					result=null;
					break;
				}
				result.add(new KeyValueEntry(key,getByteBuffer(foundData)));
				
		        status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
			}
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		} finally {
			try {
			cursor.close();
			} catch (DatabaseException e) {
				throw new GraphStorageException(e);
			}
		}
		return result;
	}

	@Override
	public void insert(List<KeyValueEntry> entries, TransactionHandle txh) {
		insert(entries,txh,true);
	}
	
	public void insert(List<KeyValueEntry> entries, TransactionHandle txh, boolean allowOverwrite) {
		Transaction tx = getTransaction(txh);
		try {
			log.debug("Preparing multiple entries: {}",entries.size());
			MultipleKeyDataEntry dbentries = new MultipleKeyDataEntry();
			for (KeyValueEntry entry : entries) {
				dbentries.append(getDataEntry(entry.getKey()), getDataEntry(entry.getValue()));
			}
			
			log.debug("Inserting multiple entries: {}",entries.size());
			OperationStatus status = db.putMultipleKey(tx, dbentries, allowOverwrite);
			
			if (status!=OperationStatus.SUCCESS) {
				if (status==OperationStatus.KEYEXIST) {
					throw new GraphStorageException("Key already exists on no-overwrite.");
				} else {
					throw new GraphStorageException("Could not write entity, return status: "+ status);
				}
			}
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}		
	}
	


	@Override
	public void delete(List<ByteBuffer> keys, TransactionHandle txh) {
		Transaction tx = getTransaction(txh);
		try {
			MultipleDataEntry dbkeys = new MultipleDataEntry();
			for (ByteBuffer entry : keys) {
				dbkeys.append(getDataEntry(entry));
			}
			
			OperationStatus status = db.deleteMultiple(tx, dbkeys);
			if (status!=OperationStatus.SUCCESS) {
				throw new GraphStorageException("Could not delete: " + status);
			}
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
		
	}
	

	
	
	
//	private<O> void putInternal(K key, O obj, EntityConverter<K,O> converter, 
//				boolean allowOverwrite, TransactionHandle txh) {
//		Transaction tx = getTransaction(txh);
//		try {
//			DatabaseEntry dbkey = getKeyEntry(key);
//			DatabaseEntry data = getDataEntry(obj,key,converter);			
//
//			
//			OperationStatus status = null;
//			if (allowOverwrite) {
//				status = db.put(tx, dbkey, data);
//			} else {
//				status = db.putNoOverwrite(tx, dbkey, data);
//			}
//			
//			if (status!=OperationStatus.SUCCESS) {
//				if (status==OperationStatus.KEYEXIST) {
//					throw new DuplicateEntryException("Key already exists on no-overwrite.");
//				} else {
//					throw new COSIException("Could not write entity, return status: "+ status);
//				}
//			}
//		} catch (DatabaseException e) {
//			throw new GraphStorageException(e);
//		}		
//	}
	



//	@Override
//	public boolean delete(K key, TransactionHandle txh) {
//		Transaction tx = getTransaction(txh);
//		try {
//			DatabaseEntry dbkey = getKeyEntry(key);
//			if (db.delete(tx, dbkey)==OperationStatus.SUCCESS) {
//				return true;
//			} else {
//				return false;
//			}
//		} catch (DatabaseException e) {
//			throw new GraphStorageException(e);
//		}
//		
//	}
	

	

	
	private final static DatabaseEntry getDataEntry(ByteBuffer key) {
		assert key.position()==0;
		DatabaseEntry dbkey = new DatabaseEntry(key);
		return dbkey;
	}
	
	private final static ByteBuffer getByteBuffer(DatabaseEntry entry) {
		ByteBuffer buffer = entry.getDataNIO();
		buffer.rewind();
		return buffer;
	}




	
}
