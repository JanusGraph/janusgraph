package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

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
    public TransactionBuilder setCacheSize(int size);

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
     * Whether to enable Metrics for this transaction, and if so, what string
     * should start the transaction's metric names.
     * <p>
     * If null, Metrics collection is totally disabled for this transaction.
     * <p>
     * If empty, Metrics collection is enabled, but there will be no prefix.
     * Where the default setting would generate metrics names in the form
     * "prefix.x.y.z", this transaction will instead use metric names in the
     * form "x.y.z".
     * <p>
     * If nonempty, Metrics collection is enabled and the prefix will be used
     * for all of this transaction's measurements.
     * <p>
     * Note: setting this to a non-null value only partially overrides
     * {@link GraphDatabaseConfiguration#BASIC_METRICS} = false in the graph
     * database configuration. When Metrics are disabled at the graph level and
     * enabled at the transaction level, storage backend timings and counters
     * will remain disabled.
     * <p>
     * The default value is
     * {@link GraphDatabaseConfiguration#METRICS_PREFIX_DEFAULT}.
     * 
     * Sets the name prefix used for Metrics recorded by this transaction. If
     * metrics is enabled via {@link GraphDatabaseConfiguration#BASIC_METRICS},
     * this string will be prepended to all Titan metric names.
     * 
     * @param prefix
     *            Metric name prefix for this transaction
     * @return
     */
    public TransactionBuilder setMetricsPrefix(String prefix);

    /**
     * Starts and returns the transaction build by this builder
     *
     * @return A new transaction configured according to this builder
     */
    public TitanTransaction start();

}
