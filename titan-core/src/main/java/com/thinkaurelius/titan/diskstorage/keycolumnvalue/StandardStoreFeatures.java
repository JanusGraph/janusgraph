package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;

/**
 * Immutable, {@link Builder}-customizable implementation of StoreFeatures.
 */
public class StandardStoreFeatures implements StoreFeatures {

    private final boolean unorderedScan;
    private final boolean orderedScan;
    private final boolean multiQuery;
    private final boolean locking;
    private final boolean batchMutation;
    private final boolean localKeyPartition;
    private final boolean keyOrdered;
    private final boolean distributed;
    private final boolean transactional;
    private final boolean keyConsistent;
    private final boolean timestamps;
    private final TimestampProviders preferredTimestamps;
    private final boolean cellLevelTTL;
    private final boolean storeLevelTTL;
    private final boolean visibility;
    private final boolean supportsPersist;
    private final Configuration keyConsistentTxConfig;
    private final Configuration localKeyConsistentTxConfig;
    private final Configuration scanTxConfig;
    private final boolean supportsInterruption;

    @Override
    public boolean hasScan() {
        return hasOrderedScan() || hasUnorderedScan();
    }

    @Override
    public boolean hasUnorderedScan() {
        return unorderedScan;
    }

    @Override
    public boolean hasOrderedScan() {
        return orderedScan;
    }

    @Override
    public boolean hasMultiQuery() {
        return multiQuery;
    }

    @Override
    public boolean hasLocking() {
        return locking;
    }

    @Override
    public boolean hasBatchMutation() {
        return batchMutation;
    }

    @Override
    public boolean isKeyOrdered() {
        return keyOrdered;
    }

    @Override
    public boolean isDistributed() {
        return distributed;
    }

    @Override
    public boolean hasTxIsolation() {
        return transactional;
    }

    @Override
    public boolean isKeyConsistent() {
        return keyConsistent;
    }

    @Override
    public boolean hasTimestamps() {
        return timestamps;
    }

    @Override
    public TimestampProviders getPreferredTimestamps() {
        return preferredTimestamps;
    }

    @Override
    public boolean hasCellTTL() {
        return cellLevelTTL;
    }

    @Override
    public boolean hasStoreTTL() {
        return storeLevelTTL;
    }

    @Override
    public boolean hasVisibility() {
        return visibility;
    }

    @Override
    public boolean supportsPersistence() { return supportsPersist; }

    @Override
    public Configuration getKeyConsistentTxConfig() {
        return keyConsistentTxConfig;
    }

    @Override
    public Configuration getLocalKeyConsistentTxConfig() {
        return localKeyConsistentTxConfig;
    }

    @Override
    public Configuration getScanTxConfig() {
        return scanTxConfig;
    }

    @Override
    public boolean hasLocalKeyPartition() {
        return localKeyPartition;
    }

    @Override
    public boolean supportsInterruption()
    {
        return supportsInterruption;
    }

    /**
     * The only way to instantiate {@link StandardStoreFeatures}.
     */
    public static class Builder {

        private boolean unorderedScan;
        private boolean orderedScan;
        private boolean multiQuery;
        private boolean locking;
        private boolean batchMutation;
        private boolean localKeyPartition;
        private boolean keyOrdered;
        private boolean distributed;
        private boolean transactional;
        private boolean timestamps;
        private TimestampProviders preferredTimestamps;
        private boolean cellLevelTTL;
        private boolean storeLevelTTL;
        private boolean visibility;
        private boolean supportsPersist = true;
        private boolean keyConsistent;
        private Configuration keyConsistentTxConfig;
        private Configuration localKeyConsistentTxConfig;
        private Configuration scanTxConfig;
        private boolean supportsInterruption = true;

        /**
         * Construct a Builder with everything disabled/unsupported/false/null.
         */
        public Builder() { }

        /**
         * Construct a Builder whose default values exactly match the values on
         * the supplied {@code template}.
         */
        public Builder(StoreFeatures template) {
            unorderedScan(template.hasUnorderedScan());
            orderedScan(template.hasOrderedScan());
            multiQuery(template.hasMultiQuery());
            locking(template.hasLocking());
            batchMutation(template.hasBatchMutation());
            localKeyPartition(template.hasLocalKeyPartition());
            keyOrdered(template.isKeyOrdered());
            distributed(template.isDistributed());
            transactional(template.hasTxIsolation());
            timestamps(template.hasTimestamps());
            preferredTimestamps(template.getPreferredTimestamps());
            cellTTL(template.hasCellTTL());
            storeTTL(template.hasStoreTTL());
            visibility(template.hasVisibility());
            persists(template.supportsPersistence());
            if (template.isKeyConsistent()) {
                keyConsistent(template.getKeyConsistentTxConfig(), template.getLocalKeyConsistentTxConfig());
            }
            scanTxConfig(template.getScanTxConfig());
            supportsInterruption(template.supportsInterruption());
        }

        public Builder unorderedScan(boolean b) {
            unorderedScan = b;
            return this;
        }

        public Builder orderedScan(boolean b) {
            orderedScan = b;
            return this;
        }

        public Builder multiQuery(boolean b) {
            multiQuery = b;
            return this;
        }

        public Builder locking(boolean b) {
            locking = b;
            return this;
        }

        public Builder batchMutation(boolean b) {
            batchMutation = b;
            return this;
        }

        public Builder localKeyPartition(boolean b) {
            localKeyPartition = b;
            return this;
        }

        public Builder keyOrdered(boolean b) {
            keyOrdered = b;
            return this;
        }

        public Builder distributed(boolean b) {
            distributed = b;
            return this;
        }

        public Builder transactional(boolean b) {
            transactional = b;
            return this;
        }

        public Builder timestamps(boolean b) {
            timestamps = b;
            return this;
        }

        public Builder preferredTimestamps(TimestampProviders t) {
            preferredTimestamps = t;
            return this;
        }

        public Builder cellTTL(boolean b) {
            cellLevelTTL = b;
            return this;
        }

        public Builder storeTTL(boolean b) {
            storeLevelTTL = b;
            return this;
        }


        public Builder visibility(boolean b) {
            visibility = b;
            return this;
        }

        public Builder persists(boolean b) {
            supportsPersist = b;
            return this;
        }

        public Builder keyConsistent(Configuration c) {
            keyConsistent = true;
            keyConsistentTxConfig = c;
            return this;
        }

        public Builder keyConsistent(Configuration global, Configuration local) {
            keyConsistent = true;
            keyConsistentTxConfig = global;
            localKeyConsistentTxConfig = local;
            return this;
        }

        public Builder notKeyConsistent() {
            keyConsistent = false;
            return this;
        }

        public Builder scanTxConfig(Configuration c) {
            scanTxConfig = c;
            return this;
        }

        public Builder supportsInterruption(boolean i)
        {
            supportsInterruption = i;
            return this;
        }

        public StandardStoreFeatures build() {
            return new StandardStoreFeatures(unorderedScan, orderedScan,
                    multiQuery, locking, batchMutation, localKeyPartition,
                    keyOrdered, distributed, transactional, keyConsistent,
                    timestamps, preferredTimestamps, cellLevelTTL,
                    storeLevelTTL, visibility, supportsPersist,
                    keyConsistentTxConfig,
                    localKeyConsistentTxConfig, scanTxConfig, supportsInterruption);
        }
    }

    private StandardStoreFeatures(boolean unorderedScan, boolean orderedScan,
            boolean multiQuery, boolean locking, boolean batchMutation,
            boolean localKeyPartition, boolean keyOrdered, boolean distributed,
            boolean transactional, boolean keyConsistent,
            boolean timestamps, TimestampProviders preferredTimestamps,
            boolean cellLevelTTL, boolean storeLevelTTL,
            boolean visibility, boolean supportsPersist,
            Configuration keyConsistentTxConfig,
            Configuration localKeyConsistentTxConfig,
            Configuration scanTxConfig, boolean supportsInterruption) {
        this.unorderedScan = unorderedScan;
        this.orderedScan = orderedScan;
        this.multiQuery = multiQuery;
        this.locking = locking;
        this.batchMutation = batchMutation;
        this.localKeyPartition = localKeyPartition;
        this.keyOrdered = keyOrdered;
        this.distributed = distributed;
        this.transactional = transactional;
        this.keyConsistent = keyConsistent;
        this.timestamps = timestamps;
        this.preferredTimestamps = preferredTimestamps;
        this.cellLevelTTL = cellLevelTTL;
        this.storeLevelTTL = storeLevelTTL;
        this.visibility = visibility;
        this.supportsPersist = supportsPersist;
        this.keyConsistentTxConfig = keyConsistentTxConfig;
        this.localKeyConsistentTxConfig = localKeyConsistentTxConfig;
        this.scanTxConfig = scanTxConfig;
        this.supportsInterruption = supportsInterruption;
    }
}
