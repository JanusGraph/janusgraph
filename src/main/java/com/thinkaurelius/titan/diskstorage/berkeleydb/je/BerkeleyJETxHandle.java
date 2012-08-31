package com.thinkaurelius.titan.diskstorage.berkeleydb.je;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BerkeleyJETxHandle implements TransactionHandle {

	private static final Logger log = LoggerFactory.getLogger(BerkeleyJETxHandle.class);
	
	private Transaction tx;
	private final boolean isReadOnly;
	
	public BerkeleyJETxHandle(Transaction t, boolean readOnly) {
		isReadOnly = readOnly;
		tx = t;
	}
	
	public BerkeleyJETxHandle(Transaction t) {
		this(t,false);
	}
	
	
	public Transaction getTransaction() {
		return tx;
	}
	
	@Override
	public void abort() throws StorageException {
	    if (tx==null) return;
		try {
	    	tx.abort();
	    	tx=null;
		} catch(DatabaseException e) {
			throw new PermanentStorageException(e);
		}
	}
	
	@Override
	public void commit() throws StorageException {
	    if (tx==null) return;
		try {
            tx.commit();
            tx=null;
		} catch(DatabaseException e) {
            throw new PermanentStorageException(e);
		}		
	}


	@Override
	public boolean isReadOnly() {
		return isReadOnly;
	}

}
