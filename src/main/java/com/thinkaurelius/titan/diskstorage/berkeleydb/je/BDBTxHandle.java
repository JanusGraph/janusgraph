package com.thinkaurelius.titan.diskstorage.berkeleydb.je;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BDBTxHandle implements TransactionHandle {

	private static final Logger log = LoggerFactory.getLogger(BDBTxHandle.class);
	
	private Transaction tx;
	private final boolean isReadOnly;
	
	public BDBTxHandle(Transaction t, boolean readOnly) {
		isReadOnly = readOnly;
		tx = t;
	}
	
	public BDBTxHandle(Transaction t) {
		this(t,false);
	}
	
	
	public Transaction getTransaction() {
		return tx;
	}
	
	@Override
	public void abort() {
	    if (tx==null) return;
		try {
	    	tx.abort();
	    	tx=null;
		} catch(DatabaseException e) {
			log.warn("Transaction could not be aborted: {}",e.getMessage());
		}
	}
	
	@Override
	public void commit() {
	    if (tx==null) return;
		try {
			try {
			    tx.commit();
			} catch (DatabaseException e) {
				log.warn("Transaction could not be committed: {}",e.getMessage());
			    if (tx != null) {
			        tx.abort();
			        tx = null;
			    }
			}
		} catch(DatabaseException e) {
			log.warn("Transaction could not be aborted: {}",e.getMessage());
		}		
	}


	@Override
	public boolean isReadOnly() {
		return isReadOnly;
	}

}
