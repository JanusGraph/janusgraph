package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.google.common.collect.Lists;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class BerkeleyJETx extends AbstractStoreTransaction {

	private static final Logger log = LoggerFactory.getLogger(BerkeleyJETx.class);
	
	private Transaction tx;
    private Set<RecordIterator> openIterators = null;
	
	public BerkeleyJETx(Transaction t, ConsistencyLevel level) {
        super(level);
		tx = t;
	}

	public Transaction getTransaction() {
		return tx;
	}

    synchronized void registerIterator(RecordIterator<?> iterator) {
        if (openIterators==null) openIterators = new HashSet<RecordIterator>();
        openIterators.add(iterator);
    }
    
    synchronized void unregisterIterator(RecordIterator<?> iterator) {
        if (openIterators!=null) openIterators.remove(iterator);
    }
    
    private void closeOpenIterators() throws StorageException {
        if (openIterators!=null) {
            for (RecordIterator iterator : Lists.newArrayList(openIterators)) { //copied to avoid ConcurrentmodificationException
                iterator.close();
            }
            assert openIterators.isEmpty();
        }
    }
	
	@Override
	public synchronized void abort() throws StorageException {
	    if (tx==null) return;
		try {
            closeOpenIterators();
	    	tx.abort();
	    	tx=null;
		} catch(DatabaseException e) {
			throw new PermanentStorageException(e);
		}
	}
	
	@Override
	public synchronized void commit() throws StorageException {
	    if (tx==null) return;
		try {
            closeOpenIterators();
            tx.commit();
            tx=null;
		} catch(DatabaseException e) {
            throw new PermanentStorageException(e);
		}		
	}


}
