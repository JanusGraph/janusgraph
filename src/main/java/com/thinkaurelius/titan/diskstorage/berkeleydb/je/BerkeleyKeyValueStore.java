package com.thinkaurelius.titan.diskstorage.berkeleydb.je;

import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
		log.trace("Contains query");
		Transaction tx = getTransaction(txh);
		try {
			DatabaseEntry dbkey = getDataEntry(key);
			DatabaseEntry data = new DatabaseEntry();
			
			OperationStatus status = db.get(tx, dbkey, data, LockMode.DEFAULT);
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
		log.trace("Get query");
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
	public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, TransactionHandle txh) {
		if (getTransaction(txh)==null) {
            throw new GraphStorageException("Enable transaction for locking in BerkeleyDB!");
		} //else we need no locking
	}


	@Override
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startInclusive, boolean endInclusive, TransactionHandle txh) {
		return getSlice(keyStart,keyEnd,startInclusive,endInclusive,Integer.MAX_VALUE,txh);
	}
	
	@Override
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startInclusive, boolean endInclusive, int limit, TransactionHandle txh) {
		return getSlice(keyStart,keyEnd,startInclusive,endInclusive,new LimitedSelector(limit),txh);
	}
	
	@Override
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startInclusive, boolean endInclusive, 
			KeySelector selector, TransactionHandle txh) {
		log.trace("Get slice query");
		Transaction tx =getTransaction(txh);
		Cursor cursor = null;
		List<KeyValueEntry> result;
		try {
			//log.debug("Sta: {}",ByteBufferUtil.toBitString(keyStart, " "));
			//log.debug("End: {}",ByteBufferUtil.toBitString(keyEnd, " "));
			
			DatabaseEntry foundKey = getDataEntry(keyStart);
			DatabaseEntry foundData = new DatabaseEntry();
			
			cursor = db.openCursor(tx, null);
			OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT);
			result = new ArrayList<KeyValueEntry>();
			//Iterate until given condition is satisfied or end of records
			while (status == OperationStatus.SUCCESS) {
				
				ByteBuffer key = getByteBuffer(foundKey);
				//log.debug("Fou: {}",ByteBufferUtil.toBitString(key, " "));
				//keyEnd.rewind();
				if (!ByteBufferUtil.isSmallerThanWithEqual(key, keyEnd, endInclusive)) break;
				//key.rewind();
				
				boolean skip = false;
				
				if (!startInclusive) {
					skip =  keyStart.equals(key);
				}
				
				if (!skip) {
					skip = !selector.include(key);
					
					if (!skip) {
						result.add(new KeyValueEntry(key,getByteBuffer(foundData)));
					}
				}
				if (selector.reachedLimit()) {
					break;
				}
		        status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
			}
			log.trace("Retrieved: {}",result.size());
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
		log.trace("Insertion");
		Transaction tx = getTransaction(txh);
		try {
			log.trace("Inserting multiple entries: {}",entries.size());
			for (KeyValueEntry entry : entries) {
				//log.debug("Key: {}",ByteBufferUtil.toBitString(entry.getKey(), " "));
				
				OperationStatus status = db.put(tx, getDataEntry(entry.getKey()), getDataEntry(entry.getValue()));
				
				if (status!=OperationStatus.SUCCESS) {
					if (status==OperationStatus.KEYEXIST) {
						throw new GraphStorageException("Key already exists on no-overwrite.");
					} else {
						throw new GraphStorageException("Could not write entity, return status: "+ status);
					}
				}
			}
		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}		
	}
	


	@Override
	public void delete(List<ByteBuffer> keys, TransactionHandle txh) {
		log.trace("Deletion");
		Transaction tx = getTransaction(txh);
		try {
			log.trace("Removing multiple keys: {}",keys.size());
			for (ByteBuffer entry : keys) {
				OperationStatus status = db.delete(tx, getDataEntry(entry));
				if (status!=OperationStatus.SUCCESS) {
					throw new GraphStorageException("Could not delete: " + status);
				}
			}
			

		} catch (DatabaseException e) {
			throw new GraphStorageException(e);
		}
		
	}
	

	private final static DatabaseEntry getDataEntry(ByteBuffer key) {
		assert key.position()==0;
		DatabaseEntry dbkey = new DatabaseEntry(key.array(),key.arrayOffset(),key.arrayOffset()+key.remaining());
		return dbkey;
	}
	
	private final static ByteBuffer getByteBuffer(DatabaseEntry entry) {
		ByteBuffer buffer = ByteBuffer.wrap(entry.getData(), entry.getOffset(), entry.getSize());
		buffer.rewind();
		return buffer;
	}




	
}
