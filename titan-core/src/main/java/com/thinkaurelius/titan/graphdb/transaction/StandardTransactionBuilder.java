package com.thinkaurelius.titan.graphdb.transaction;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TITAN_NS;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.diskstorage.configuration.UserModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionConfig;
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

    private boolean hasEnabledBatchLoading = false;

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

    private String logIdentifier;

    private Long timestamp = null;

    private String metricsPrefix;

    private final UserModifiableConfiguration storageConfiguration;

    private final StandardTitanGraph graph;

    private final TimeUnit graphTimeUnit;

    /**
     * Constructs a new TitanTransaction configuration with default configuration parameters.
     */
    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardTitanGraph graph) {
        Preconditions.checkNotNull(graphConfig);
        Preconditions.checkNotNull(graph);
        this.graph = graph;
        this.graphTimeUnit = graphConfig.getTimestampProvider().getUnit();
        this.defaultTypeMaker = graphConfig.getDefaultTypeMaker();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.metricsPrefix = graphConfig.getMetricsPrefix();
        this.logIdentifier = null;
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.storageConfiguration = new UserModifiableConfiguration(GraphDatabaseConfiguration.buildConfiguration());
        if (graphConfig.isReadOnly()) readOnly();
        setCacheSize(graphConfig.getTxCacheSize());
        if (graphConfig.isBatchLoading()) enableBatchLoading();
    }

    public StandardTransactionBuilder threadBound() {
        this.threadBound = true;
        this.singleThreaded = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder readOnly() {
        this.isReadOnly = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder enableBatchLoading() {
        hasEnabledBatchLoading = true;
        verifyUniqueness = false;
        verifyExternalVertexExistence = false;
        acquireLocks = false;
        return this;
    }

    @Override
    public StandardTransactionBuilder setCacheSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.vertexCacheSize = size;
        this.indexCacheWeight = size / 2;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkInternalVertexExistence() {
        this.verifyInternalVertexExistence = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder setTimestamp(long timestampSinceEpoch, TimeUnit unit) {
        this.timestamp = graphTimeUnit.convert(timestampSinceEpoch, unit);
        return this;
    }

    @Override
    public StandardTransactionBuilder setMetricsPrefix(String p) {
        this.metricsPrefix = p;
        return this;
    }

    @Override
    public StandardTransactionBuilder setLogIdentifier(String logName) {
        this.logIdentifier = logName;
        return this;
    }

    @Override
    public TransactionBuilder setCustomOption(String k, Object v) {
        storageConfiguration.set(k, v);
        return this;
    }

    @Override
    public TitanTransaction start() {
        TransactionConfiguration immutable = new ImmutableTxCfg(isReadOnly, hasEnabledBatchLoading,
                assignIDsImmediately, verifyExternalVertexExistence,
                verifyInternalVertexExistence, acquireLocks, verifyUniqueness,
                propertyPrefetching, singleThreaded, threadBound,
                timestamp,
                indexCacheWeight, vertexCacheSize, logIdentifier, metricsPrefix,
                defaultTypeMaker, new BasicConfiguration(TITAN_NS,
                        storageConfiguration.getConfiguration(),
                        Restriction.NONE));
        return graph.newTransaction(immutable);
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
    public boolean hasEnabledBatchLoading() {
        return hasEnabledBatchLoading;
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
    public String getLogIdentifier() {
        return logIdentifier;
    }

    @Override
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    @Override
    public boolean hasMetricsPrefix() {
        return null != metricsPrefix;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public <V> V getCustomOption(ConfigOption<V> opt) {
        return getCustomOptions().get(opt);
    }

    @Override
    public Configuration getCustomOptions() {
        return new BasicConfiguration(TITAN_NS,
                storageConfiguration.getConfiguration(), Restriction.NONE);
    }

    private static class ImmutableTxCfg implements TransactionConfiguration {

        private final boolean isReadOnly;
        private final boolean hasEnabledBatchLoading;
        private final boolean hasAssignIDsImmediately;
        private final boolean hasVerifyExternalVertexExistence;
        private final boolean hasVerifyInternalVertexExistence;
        private final boolean hasAcquireLocks;
        private final boolean hasVerifyUniqueness;
        private final boolean hasPropertyPrefetching;
        private final boolean isSingleThreaded;
        private final boolean isThreadBound;
        private final long indexCacheWeight;
        private final int vertexCacheSize;
        private final String logIdentifier;
        private final DefaultTypeMaker defaultTypeMaker;

        private final TransactionHandleConfig handleConfig;

        public ImmutableTxCfg(boolean isReadOnly,
                boolean hasEnabledBatchLoading,
                boolean hasAssignIDsImmediately,
                boolean hasVerifyExternalVertexExistence,
                boolean hasVerifyInternalVertexExistence,
                boolean hasAcquireLocks, boolean hasVerifyUniqueness,
                boolean hasPropertyPrefetching, boolean isSingleThreaded,
                boolean isThreadBound, Long timestamp,
                long indexCacheWeight, int vertexCacheSize, String logIdentifier,
                String metricsPrefix, DefaultTypeMaker defaultTypeMaker,
                Configuration storageConfiguration) {
            this.isReadOnly = isReadOnly;
            this.hasEnabledBatchLoading = hasEnabledBatchLoading;
            this.hasAssignIDsImmediately = hasAssignIDsImmediately;
            this.hasVerifyExternalVertexExistence = hasVerifyExternalVertexExistence;
            this.hasVerifyInternalVertexExistence = hasVerifyInternalVertexExistence;
            this.hasAcquireLocks = hasAcquireLocks;
            this.hasVerifyUniqueness = hasVerifyUniqueness;
            this.hasPropertyPrefetching = hasPropertyPrefetching;
            this.isSingleThreaded = isSingleThreaded;
            this.isThreadBound = isThreadBound;
            this.indexCacheWeight = indexCacheWeight;
            this.vertexCacheSize = vertexCacheSize;
            this.logIdentifier = logIdentifier;
            this.defaultTypeMaker = defaultTypeMaker;
            this.handleConfig = new StandardTransactionConfig.Builder()
                    .timestamp(timestamp)
                    .metricsPrefix(metricsPrefix)
                    .customOptions(storageConfiguration).build();
        }

        @Override
        public boolean hasEnabledBatchLoading() {
            return hasEnabledBatchLoading;
        }

        @Override
        public boolean isReadOnly() {
            return isReadOnly;
        }

        @Override
        public boolean hasAssignIDsImmediately() {
            return hasAssignIDsImmediately;
        }

        @Override
        public boolean hasVerifyExternalVertexExistence() {
            return hasVerifyExternalVertexExistence;
        }

        @Override
        public boolean hasVerifyInternalVertexExistence() {
            return hasVerifyInternalVertexExistence;
        }

        @Override
        public boolean hasAcquireLocks() {
            return hasAcquireLocks;
        }

        @Override
        public DefaultTypeMaker getAutoEdgeTypeMaker() {
            return defaultTypeMaker;
        }

        @Override
        public boolean hasVerifyUniqueness() {
            return hasVerifyUniqueness;
        }

        @Override
        public boolean hasPropertyPrefetching() {
            return hasPropertyPrefetching;
        }

        @Override
        public boolean isSingleThreaded() {
            return isSingleThreaded;
        }

        @Override
        public boolean isThreadBound() {
            return isThreadBound;
        }

        @Override
        public int getVertexCacheSize() {
            return vertexCacheSize;
        }

        @Override
        public long getIndexCacheWeight() {
            return indexCacheWeight;
        }

        @Override
        public String getLogIdentifier() {
            return logIdentifier;
        }

        @Override
        public Long getTimestamp() {
            return handleConfig.getTimestamp();
        }

        @Override
        public String getMetricsPrefix() {
            return handleConfig.getMetricsPrefix();
        }

        @Override
        public boolean hasMetricsPrefix() {
            return handleConfig.hasMetricsPrefix();
        }

        @Override
        public <V> V getCustomOption(ConfigOption<V> opt) {
            return handleConfig.getCustomOption(opt);
        }

        @Override
        public Configuration getCustomOptions() {
            return handleConfig.getCustomOptions();
        }
    }
}
