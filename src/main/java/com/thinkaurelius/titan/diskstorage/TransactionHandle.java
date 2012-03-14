package com.thinkaurelius.titan.diskstorage;

/**
 * A transaction handle uniquely identifies a transaction on the storage backend.
 * 
 * All modifications to the storage backend must occur within the context of a single 
 * transaction. Such a transaction is identified to the Titan middleware by a TransactionHandle.
 * Graph transactions rely on the existence of a storage backend transaction.
 * 
 * Note, that a TransactionHandle by itself does not provide any isolation or consistency guarantees (e.g. ACID).
 * Graph Transactions can only extend such guarantees if they are supported by the respective storage backend.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public interface TransactionHandle {
	
	public static final TransactionHandle NoTransaction = new TransactionHandle(){

		@Override
		public void abort() {
			throw new UnsupportedOperationException("Cannot abort a non-transaction!");
		}

		@Override
		public void commit() {
			//Do nothing
		}

		@Override
		public boolean isReadOnly() {
			return false;
		}

		
	};
	
	/**
	 * Commits the transaction.
	 */
	public void commit();
	
	/**
	 * Aborts (or rolls back) the transaction.
	 */
	public void abort();
	
	/**
	 * Checks whether the transaction identified by this handle is read only.
	 * 
	 * @return True, if read only, else False.
	 */
	public boolean isReadOnly();
	
}
