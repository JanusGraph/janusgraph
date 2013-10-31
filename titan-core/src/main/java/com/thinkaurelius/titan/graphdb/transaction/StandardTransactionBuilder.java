package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

/**
 * Used to configure a {@link com.thinkaurelius.titan.core.TitanTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see com.thinkaurelius.titan.core.TitanTransaction
 */
public class StandardTransactionBuilder implements TransactionConfiguration, TransactionBuilder {

    private boolean isReadOnly = false;

    private boolean assignIDsImmediately = false;

    private DefaultTypeMaker defaultTypeMaker;

    private boolean verifyExternalVertexExistence = true;

    private boolean verifyInternalVertexExistence = false;

    private boolean verifyUniqueness = true;

    private boolean acquireLocks = true;

    private boolean propertyPrefetching = true;

    private boolean singleThreaded = false;

    private boolean threadBound = false;

    private int vertexCacheSize;

    private long indexCacheWeight;

    private Long timestamp = null;

    private String metricsPrefix;

    /**
     * Used to keep state information: Once the transaction is openend, the config
     * has to be closed to ensure its immutability
     */
    private boolean isOpen = true;

    private final StandardTitanGraph graph;

    /**
     * Constructs a new TitanTransaction configuration with default configuration parameters.
     */
    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardTitanGraph graph) {
        Preconditions.checkNotNull(graphConfig);
        Preconditions.checkNotNull(graph);
        this.graph = graph;
        this.defaultTypeMaker = graphConfig.getDefaultTypeMaker();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.metricsPrefix = graphConfig.getMetricsPrefix();
        this.propertyPrefetching = graphConfig.getPropertyPrefetching();
        if (graphConfig.isReadOnly()) readOnly();
        setCacheSize(graphConfig.getTxCacheSize());
        if (graphConfig.isBatchLoading()) enableBatchLoading();
    }

    private void verifyOpen() {
        Preconditions.checkState(isOpen, "Transaction has been started which renders this configuration immutable");
    }

    public StandardTransactionBuilder threadBound() {
        verifyOpen();
        this.threadBound = true;
        this.singleThreaded = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder readOnly() {
        verifyOpen();
        this.isReadOnly = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder enableBatchLoading() {
        verifyOpen();
        verifyUniqueness = false;
        verifyExternalVertexExistence = false;
        acquireLocks = false;
        return this;
    }

    @Override
    public StandardTransactionBuilder setCacheSize(int size) {
        verifyOpen();
        Preconditions.checkArgument(size >= 0);
        this.vertexCacheSize = size;
        this.indexCacheWeight = size / 2;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkInternalVertexExistence() {
        verifyOpen();
        this.verifyInternalVertexExistence = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder setTimestamp(long timestamp) {
        verifyOpen();
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public StandardTransactionBuilder setMetricsPrefix(String p) {
        verifyOpen();
        this.metricsPrefix = p;
        return this;
    }

    @Override
    public TitanTransaction start() {
        verifyOpen();
        isOpen = false;
        return graph.newTransaction(this);
    }


    /* ##############################################
                    TransactionConfig
    ############################################## */


    @Override
    public final boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public final boolean hasAssignIDsImmediately() {
        return assignIDsImmediately;
    }

    @Override
    public final boolean hasVerifyExternalVertexExistence() {
        return verifyExternalVertexExistence;
    }

    @Override
    public final boolean hasVerifyInternalVertexExistence() {
        return verifyInternalVertexExistence;
    }

    @Override
    public final boolean hasAcquireLocks() {
        return acquireLocks;
    }

    @Override
    public final DefaultTypeMaker getAutoEdgeTypeMaker() {
        return defaultTypeMaker;
    }

    @Override
    public final boolean hasVerifyUniqueness() {
        return verifyUniqueness;
    }

    public boolean hasPropertyPrefetching() {
        return propertyPrefetching;
    }

    @Override
    public final boolean isSingleThreaded() {
        return singleThreaded;
    }

    @Override
    public final boolean isThreadBound() {
        return threadBound;
    }

    @Override
    public final int getVertexCacheSize() {
        return vertexCacheSize;
    }

    @Override
    public final long getIndexCacheWeight() {
        return indexCacheWeight;
    }

    @Override
    public boolean hasTimestamp() {
        return timestamp != null;
    }

    @Override
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    @Override
    public long getTimestamp() {
        Preconditions.checkState(timestamp != null, "A timestamp has not been configured");
        return timestamp;
    }
}
