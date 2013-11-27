package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.util.List;
import java.util.Map;

/**
 * External index for querying.
 * An index can contain an arbitrary number of index stores which are updated and queried separately.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexProvider extends IndexInformation {

    /**
     * This method registers a new key for the specified index store with the given data type. This allows the IndexProvider
     * to prepare the index if necessary.
     *
     * It is expected that this method is first called with each new key to inform the index of the expected type before the
     * key is used in any documents.
     *
     * @param store Index store
     * @param key New key to register
     * @param information Information on the key to register
     * @param tx enclosing transaction
     * @throws StorageException
     */
    public void register(String store, String key, KeyInformation information, TransactionHandle tx) throws StorageException;

    /**
     * Mutates the index (adds and removes fields or entire documents)
     *
     * @param mutations Updates to the index. First map contains all the mutations for each store. The inner map contains
     *                  all changes for each document in an {@link IndexMutation}.
     * @param informations Information on the keys used in the mutation accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @throws StorageException
     * @see IndexMutation
     */
    public void mutate(Map<String,Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException;

    /**
     * Executes the given query against the index.
     *
     * @param query Query to execute
     * @param informations Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return The ids of all matching documents
     * @throws StorageException
     * @see IndexQuery
     */
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException;


    /**
     * Executes the given raw query against the index
     *
     * @param query Query to execute
     * @param informations Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return Results objects for all matching documents (i.e. document id and score)
     * @throws StorageException
     * @see RawQuery
     */
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException;

    /**
     * Returns a transaction handle for a new index transaction.
     *
     * @return New Transaction Handle
     */
    public TransactionHandle beginTransaction() throws StorageException;

    /**
     * Closes the index
     * @throws StorageException
     */
    public void close() throws StorageException;

    /**
     * Clears the index and removes all entries in all stores.
     * @throws StorageException
     */
    public void clearStorage() throws StorageException;

}
