package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.util.List;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexProvider extends IndexInformation {

    /**
     * It is expected that this method is first called with each new key to inform the index of the expected type.
     *
     * @param key
     * @param dataType
     */
    public void register(String store, String key, Class<?> dataType, TransactionHandle tx) throws StorageException;

    public void mutate(Map<String,Map<String, IndexMutation>> mutations, TransactionHandle tx) throws StorageException;

    public List<String> query(IndexQuery query, TransactionHandle tx) throws StorageException;

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Handle
     */
    public TransactionHandle beginTransaction() throws StorageException;

    public void close() throws StorageException;

    public void clearStorage() throws StorageException;

}
