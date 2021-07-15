// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.transaction;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.TransactionBuilder;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.time.Instant;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;

/**
 * Used to configure a {@link org.janusgraph.core.JanusGraphTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see org.janusgraph.core.JanusGraphTransaction
 */
public class StandardTransactionBuilder implements TransactionConfiguration, TransactionBuilder {

    private boolean isReadOnly = false;

    private boolean hasEnabledBatchLoading = false;

    private final boolean assignIDsImmediately;

    private boolean preloadedData = false;

    private final DefaultSchemaMaker defaultSchemaMaker;

    private boolean hasDisabledSchemaConstraints = true;

    private boolean verifyExternalVertexExistence = true;

    private boolean verifyInternalVertexExistence = false;

    private boolean verifyUniqueness = true;

    private boolean acquireLocks = true;

    private boolean propertyPrefetching;

    private boolean multiQuery;

    private boolean singleThreaded = false;

    private boolean threadBound = false;

    private int vertexCacheSize;

    private int dirtyVertexSize;

    private long indexCacheWeight;

    private String logIdentifier;

    private int[] restrictedPartitions = new int[0];

    private Instant userCommitTime = null;

    private String groupName;

    private final boolean forceIndexUsage;

    private final ModifiableConfiguration writableCustomOptions;

    private final Configuration customOptions;

    private final StandardJanusGraph graph;

    /**
     * Constructs a new JanusGraphTransaction configuration with default configuration parameters.
     */
    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardJanusGraph graph) {
        Preconditions.checkNotNull(graphConfig);
        Preconditions.checkNotNull(graph);
        if (graphConfig.isReadOnly()) readOnly();
        if (graphConfig.isBatchLoading()) enableBatchLoading();
        this.graph = graph;
        this.defaultSchemaMaker = graphConfig.getDefaultSchemaMaker();
        this.hasDisabledSchemaConstraints = graphConfig.hasDisabledSchemaConstraints();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.forceIndexUsage = graphConfig.hasForceIndexUsage();
        this.groupName = graphConfig.getMetricsPrefix();
        this.logIdentifier = null;
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.multiQuery = graphConfig.useMultiQuery();
        this.writableCustomOptions = GraphDatabaseConfiguration.buildGraphConfiguration();
        this.customOptions = new MergedConfiguration(writableCustomOptions, graphConfig.getConfiguration());
        vertexCacheSize(graphConfig.getTxVertexCacheSize());
        dirtyVertexSize(graphConfig.getTxDirtyVertexSize());
    }

    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardJanusGraph graph, Configuration customOptions) {
        Preconditions.checkNotNull(graphConfig);
        Preconditions.checkNotNull(graph);
        if (graphConfig.isReadOnly()) readOnly();
        if (graphConfig.isBatchLoading()) enableBatchLoading();
        this.graph = graph;
        this.defaultSchemaMaker = graphConfig.getDefaultSchemaMaker();
        this.hasDisabledSchemaConstraints = graphConfig.hasDisabledSchemaConstraints();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.forceIndexUsage = graphConfig.hasForceIndexUsage();
        this.groupName = graphConfig.getMetricsPrefix();
        this.logIdentifier = null;
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.multiQuery = graphConfig.useMultiQuery();
        this.writableCustomOptions = null;
        this.customOptions = customOptions;
        vertexCacheSize(graphConfig.getTxVertexCacheSize());
        dirtyVertexSize(graphConfig.getTxDirtyVertexSize());
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
    public StandardTransactionBuilder readOnlyOLAP() {
        isReadOnly = true;
        preloadedData = true;
        verifyInternalVertexExistence = false;
        dirtyVertexSize = 0;
        vertexCacheSize = 0;
        return this;
    }

    @Override
    public StandardTransactionBuilder enableBatchLoading() {
        hasEnabledBatchLoading = true;
        checkExternalVertexExistence(false);
        consistencyChecks(false);
        return this;
    }

    @Override
    public StandardTransactionBuilder disableBatchLoading() {
        hasEnabledBatchLoading = false;
        checkExternalVertexExistence(true);
        consistencyChecks(true);
        return this;
    }

    @Override
    public StandardTransactionBuilder propertyPrefetching(boolean enabled) {
        propertyPrefetching = enabled;
        return this;
    }

    @Override
    public StandardTransactionBuilder multiQuery(boolean enabled) {
        multiQuery = enabled;
        return this;
    }

    @Override
    public StandardTransactionBuilder vertexCacheSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.vertexCacheSize = size;
        this.indexCacheWeight = size / 2;
        return this;
    }

    @Override
    public TransactionBuilder dirtyVertexSize(int size) {
        this.dirtyVertexSize = size;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkInternalVertexExistence(boolean enabled) {
        this.verifyInternalVertexExistence = enabled;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkExternalVertexExistence(boolean enabled) {
        this.verifyExternalVertexExistence = enabled;
        return this;
    }

    @Override
    public TransactionBuilder consistencyChecks(boolean enabled) {
        this.verifyUniqueness = enabled;
        this.acquireLocks = enabled;
        return this;
    }

    @Override
    public StandardTransactionBuilder commitTime(Instant timestampSinceEpoch) {
        this.userCommitTime = timestampSinceEpoch;
        return this;
    }

    @Override
    public void setCommitTime(Instant time) {
        throw new UnsupportedOperationException("Use setCommitTime(long,TimeUnit)");
    }

    @Override
    public StandardTransactionBuilder groupName(String p) {
        this.groupName = p;
        return this;
    }

    @Override
    public StandardTransactionBuilder logIdentifier(String logName) {
        this.logIdentifier = logName;
        return this;
    }

    @Override
    public TransactionBuilder restrictedPartitions(int[] partitions) {
        Preconditions.checkNotNull(partitions);
        this.restrictedPartitions=partitions;
        return this;
    }

    public TransactionBuilder setPreloadedData(boolean preloaded) {
        this.preloadedData = preloaded;
        return this;
    }


    @Override
    public TransactionBuilder customOption(String k, Object v) {
        if (null == writableCustomOptions)
            throw new IllegalStateException("This builder was not constructed with setCustomOption support");
        writableCustomOptions.set((ConfigOption<Object>)ConfigElement.parse(ROOT_NS, k).element, v);
        return this;
    }

    @Override
    public JanusGraphTransaction start() {
        TransactionConfiguration immutable = new ImmutableTxCfg(isReadOnly, hasEnabledBatchLoading,
                assignIDsImmediately, preloadedData, forceIndexUsage, verifyExternalVertexExistence,
                verifyInternalVertexExistence, acquireLocks, verifyUniqueness,
                propertyPrefetching, multiQuery, singleThreaded, threadBound, getTimestampProvider(), userCommitTime,
                indexCacheWeight, getVertexCacheSize(), getDirtyVertexSize(),
                logIdentifier, restrictedPartitions, groupName,
                defaultSchemaMaker, hasDisabledSchemaConstraints, customOptions);
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
    public boolean hasPreloadedData() { return preloadedData; }

    @Override
    public final boolean hasForceIndexUsage() {
        return forceIndexUsage;
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
    public final DefaultSchemaMaker getAutoSchemaMaker() {
        return defaultSchemaMaker;
    }

    @Override
    public boolean hasDisabledSchemaConstraints() {
        return hasDisabledSchemaConstraints;
    }

    @Override
    public final boolean hasVerifyUniqueness() {
        return verifyUniqueness;
    }

    @Override
    public boolean hasPropertyPrefetching() {
        return propertyPrefetching;
    }

    @Override
    public boolean useMultiQuery() {
        return multiQuery;
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
    public int[] getRestrictedPartitions() {
        return restrictedPartitions;
    }

    @Override
    public boolean hasRestrictedPartitions() {
        return restrictedPartitions.length>0;
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
    public Instant getCommitTime() {
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
        return customOptions;
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return graph.getConfiguration().getTimestampProvider();
    }

    private static class ImmutableTxCfg implements TransactionConfiguration {

        private final boolean isReadOnly;
        private final boolean hasEnabledBatchLoading;
        private final boolean hasAssignIDsImmediately;
        private final boolean hasPreloadedData;
        private final boolean hasForceIndexUsage;
        private final boolean hasVerifyExternalVertexExistence;
        private final boolean hasVerifyInternalVertexExistence;
        private final boolean hasAcquireLocks;
        private final boolean hasVerifyUniqueness;
        private final boolean hasPropertyPrefetching;
        private final boolean useMultiQuery;
        private final boolean isSingleThreaded;
        private final boolean isThreadBound;
        private final long indexCacheWeight;
        private final int vertexCacheSize;
        private final int dirtyVertexSize;
        private final String logIdentifier;
        private final int[] restrictedPartitions;
        private final DefaultSchemaMaker defaultSchemaMaker;
        private boolean hasDisabledSchemaConstraints = true;

        private final BaseTransactionConfig handleConfig;

        public ImmutableTxCfg(boolean isReadOnly,
                boolean hasEnabledBatchLoading,
                boolean hasAssignIDsImmediately,
                boolean hasPreloadedData,
                boolean hasForceIndexUsage,
                boolean hasVerifyExternalVertexExistence,
                boolean hasVerifyInternalVertexExistence,
                boolean hasAcquireLocks, boolean hasVerifyUniqueness,
                boolean hasPropertyPrefetching, boolean useMultiQuery, boolean isSingleThreaded,
                boolean isThreadBound, TimestampProvider times, Instant commitTime,
                long indexCacheWeight, int vertexCacheSize, int dirtyVertexSize, String logIdentifier,
                int[] restrictedPartitions,
                String groupName,
                DefaultSchemaMaker defaultSchemaMaker,
                boolean hasDisabledSchemaConstraints,
                Configuration customOptions) {
            this.isReadOnly = isReadOnly;
            this.hasEnabledBatchLoading = hasEnabledBatchLoading;
            this.hasAssignIDsImmediately = hasAssignIDsImmediately;
            this.hasPreloadedData = hasPreloadedData;
            this.hasForceIndexUsage = hasForceIndexUsage;
            this.hasVerifyExternalVertexExistence = hasVerifyExternalVertexExistence;
            this.hasVerifyInternalVertexExistence = hasVerifyInternalVertexExistence;
            this.hasAcquireLocks = hasAcquireLocks;
            this.hasVerifyUniqueness = hasVerifyUniqueness;
            this.hasPropertyPrefetching = hasPropertyPrefetching;
            this.useMultiQuery = useMultiQuery;
            this.isSingleThreaded = isSingleThreaded;
            this.isThreadBound = isThreadBound;
            this.indexCacheWeight = indexCacheWeight;
            this.vertexCacheSize = vertexCacheSize;
            this.dirtyVertexSize = dirtyVertexSize;
            this.logIdentifier = logIdentifier;
            this.restrictedPartitions=restrictedPartitions;
            this.defaultSchemaMaker = defaultSchemaMaker;
            this.hasDisabledSchemaConstraints = hasDisabledSchemaConstraints;
            this.handleConfig = new StandardBaseTransactionConfig.Builder()
                    .commitTime(commitTime)
                    .timestampProvider(times)
                    .groupName(groupName)
                    .customOptions(customOptions).build();
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
        public boolean hasPreloadedData() {
            return hasPreloadedData;
        }

        @Override
        public final boolean hasForceIndexUsage() {
            return hasForceIndexUsage;
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
        public DefaultSchemaMaker getAutoSchemaMaker() {
            return defaultSchemaMaker;
        }

        @Override
        public boolean hasDisabledSchemaConstraints() {
            return hasDisabledSchemaConstraints;
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
        public boolean useMultiQuery() {
            return useMultiQuery;
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
        public int[] getRestrictedPartitions() {
            return restrictedPartitions;
        }

        @Override
        public boolean hasRestrictedPartitions() {
            return restrictedPartitions.length>0;
        }

        @Override
        public Instant getCommitTime() {
            return handleConfig.getCommitTime();
        }

        @Override
        public void setCommitTime(Instant time) {
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
