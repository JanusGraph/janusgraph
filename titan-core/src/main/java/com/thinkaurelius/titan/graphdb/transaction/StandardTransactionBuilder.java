package com.thinkaurelius.titan.graphdb.transaction;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.configuration.UserModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionHandleConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.util.time.TimestampProvider;

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

    private int dirtyVertexSize;

    private long indexCacheWeight;

    private String logIdentifier;

    private Timepoint userCommitTime = null;

    private String groupName;

    private final UserModifiableConfiguration storageConfiguration;

    private final StandardTitanGraph graph;

    /**
     * Constructs a new TitanTransaction configuration with default configuration parameters.
     */
    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardTitanGraph graph) {
        Preconditions.checkNotNull(graphConfig);
        Preconditions.checkNotNull(graph);
        if (graphConfig.isReadOnly()) readOnly();
        if (graphConfig.isBatchLoading()) enableBatchLoading();
        this.graph = graph;
        this.defaultTypeMaker = graphConfig.getDefaultTypeMaker();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.groupName = graphConfig.getMetricsPrefix();
        this.logIdentifier = null;
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.storageConfiguration = new UserModifiableConfiguration(GraphDatabaseConfiguration.buildConfiguration());
        setVertexCacheSize(graphConfig.getTxVertexCacheSize());
        setDirtyVertexSize(graphConfig.getTxDirtyVertexSize());
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
    public StandardTransactionBuilder setVertexCacheSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.vertexCacheSize = size;
        this.indexCacheWeight = size / 2;
        return this;
    }

    @Override
    public TransactionBuilder setDirtyVertexSize(int size) {
        this.dirtyVertexSize = size;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkInternalVertexExistence() {
        this.verifyInternalVertexExistence = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder setCommitTime(long timestampSinceEpoch, TimeUnit unit) {
        this.userCommitTime = getTimestampProvider().getTime(timestampSinceEpoch,unit);
        return this;
    }

    @Override
    public void setCommitTime(Timepoint time) {
        throw new UnsupportedOperationException("Use setCommitTime(lnog,TimeUnit)");
    }

    @Override
    public StandardTransactionBuilder setGroupName(String p) {
        this.groupName = p;
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
                propertyPrefetching, singleThreaded, threadBound, getTimestampProvider(), userCommitTime,
                indexCacheWeight, getVertexCacheSize(), getDirtyVertexSize(),
                logIdentifier, groupName,
                defaultTypeMaker, new BasicConfiguration(ROOT_NS,
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
    public final int getDirtyVertexSize() {
        return dirtyVertexSize;
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
    public String getGroupName() {
        return groupName;
    }

    @Override
    public boolean hasGroupName() {
        return null != groupName;
    }

    @Override
    public Timepoint getCommitTime() {
        return userCommitTime;
    }

    @Override
    public boolean hasCommitTime() {
        return userCommitTime!=null;
    }

    @Override
    public <V> V getCustomOption(ConfigOption<V> opt) {
        return getCustomOptions().get(opt);
    }

    @Override
    public Configuration getCustomOptions() {
        return new BasicConfiguration(ROOT_NS,
                storageConfiguration.getConfiguration(), Restriction.NONE);
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return graph.getConfiguration().getTimestampProvider();
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
        private final int dirtyVertexSize;
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
                boolean isThreadBound, TimestampProvider times, Timepoint commitTime,
                long indexCacheWeight, int vertexCacheSize, int dirtyVertexSize, String logIdentifier,
                String groupName, DefaultTypeMaker defaultTypeMaker,
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
            this.dirtyVertexSize = dirtyVertexSize;
            this.logIdentifier = logIdentifier;
            this.defaultTypeMaker = defaultTypeMaker;
            this.handleConfig = new StandardTransactionHandleConfig.Builder()
                    .commitTime(commitTime)
                    .timestampProvider(times)
                    .groupName(groupName)
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
        public int getDirtyVertexSize() {
            return dirtyVertexSize;
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
        public Timepoint getCommitTime() {
            return handleConfig.getCommitTime();
        }

        @Override
        public void setCommitTime(Timepoint time) {
            handleConfig.setCommitTime(time);
        }

        @Override
        public boolean hasCommitTime() {
            return handleConfig.hasCommitTime();
        }

        @Override
        public String getGroupName() {
            return handleConfig.getGroupName();
        }

        @Override
        public boolean hasGroupName() {
            return handleConfig.hasGroupName();
        }

        @Override
        public <V> V getCustomOption(ConfigOption<V> opt) {
            return handleConfig.getCustomOption(opt);
        }

        @Override
        public Configuration getCustomOptions() {
            return handleConfig.getCustomOptions();
        }

        @Override
        public TimestampProvider getTimestampProvider() {
            return handleConfig.getTimestampProvider();
        }
    }
}
