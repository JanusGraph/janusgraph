package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;

/**
 * Constructor returned by {@link com.thinkaurelius.titan.core.TitanGraph#buildTransaction()} to build a new transaction.
 * The TransactionBuilder allows certain aspects of the resulting transaction to be configured up-front.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionBuilder {

    /**
     * Makes the transaction read only. Any writes will cause an exception.
     * Read-only transactions do not have to maintain certain data structures and can hence be more efficient.
     *
     * @return
     */
    public TransactionBuilder readOnly();

    /**
     * Enabling batch loading disables a number of consistency checks inside Titan to speed up the ingestion of
     * data under the assumptions that inconsistencies are resolved prior to loading.
     *
     * @return
     */
    public TransactionBuilder enableBatchLoading();

    /**
     * Configures the size of the internal caches used in the transaction.
     *
     * @param size
     * @return
     */
    public TransactionBuilder setCacheSize(long size);

    /**
     * Enables checks that verify that each vertex actually exists in the underlying data store when it is retrieved.
     * This might be useful to address common data degradation issues but has adverse impacts on performance due to
     * repeated existence checks.
     *
     * @return
     */
    public TransactionBuilder checkInternalVertexExistence();

    /**
     * Sets the timestamp for this transaction. The transaction will be recorded with this timestamp
     * in those storage backends where the timestamp is recorded.
     *
     * @param timestamp
     * @return
     */
    public TransactionBuilder setTimestamp(long timestamp);

    /**
     * Starts and returns the transaction build by this builder
     *
     * @return A new transaction configured according to this builder
     */
    public TitanTransaction start();

}
