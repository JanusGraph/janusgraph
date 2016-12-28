package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransaction;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfigurable;

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException;

    /**
     * Mutates the index (adds and removes fields or entire documents)
     *
     * @param mutations Updates to the index. First map contains all the mutations for each store. The inner map contains
     *                  all changes for each document in an {@link IndexMutation}.
     * @param informations Information on the keys used in the mutation accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     * @see IndexMutation
     */
    public void mutate(Map<String,Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException;


    /**
     * Restores the index to the state of the primary data store as given in the {@code documents} variable. When this method returns, the index records
     * for the given documents exactly matches the provided data. Unlike {@link #mutate(java.util.Map, KeyInformation.IndexRetriever, BaseTransaction)}
     * this method does not do a delta-update, but entirely replaces the documents with the provided data or deletes them if the document content is empty.
     *
     * @param documents The outer map maps stores to documents, the inner contains the documents mapping document ids to the document content which is a
     *                  list of {@link IndexEntry}. If that list is empty, that means this document should not exist and ought to be deleted.
     * @param informations Information on the keys used in the mutation accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @throws BackendException
     */
    public void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException;


    /**
     * Executes the given query against the index.
     *
     * @param query Query to execute
     * @param informations Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return The ids of all matching documents
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     * @see IndexQuery
     */
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException;


    /**
     * Executes the given raw query against the index
     *
     * @param query Query to execute
     * @param informations Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return Results objects for all matching documents (i.e. document id and score)
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     * @see RawQuery
     */
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException;

    /**
     * Returns a transaction handle for a new index transaction.
     *
     * @return New Transaction Handle
     */
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException;

    /**
     * Closes the index
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void close() throws BackendException;

    /**
     * Clears the index and removes all entries in all stores.
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void clearStorage() throws BackendException;

}
