package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;

/**
 * A transaction handle uniquely identifies a transaction on the storage backend.
 * 
 * All modifications to the storage backend must occur within the context of a single 
 * transaction. Such a transaction is identified to the Titan middleware by a StoreTransaction.
 * Graph transactions rely on the existence of a storage backend transaction.
 * 
 * Note, that a StoreTransaction by itself does not provide any isolation or consistency guarantees (e.g. ACID).
 * Graph Transactions can only extend such guarantees if they are supported by the respective storage backend.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public interface StoreTransaction extends TransactionHandle {

    public ConsistencyLevel getConsistencyLevel();

}
