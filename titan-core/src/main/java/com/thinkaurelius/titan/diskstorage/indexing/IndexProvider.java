package com.thinkaurelius.titan.diskstorage.indexing;

import com.sun.jersey.core.util.StringKeyIgnoreCaseMultivaluedMap;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexProvider {

    public void mutate(Map<String, IndexMutation> mutations, TransactionHandle tx) throws StorageException;

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Handle
     */
    public TransactionHandle beginTransaction() throws StorageException;

}
