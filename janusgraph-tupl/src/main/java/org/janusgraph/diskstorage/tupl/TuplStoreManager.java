/*
 * Copyright 2017 JanusGraph Authors
 * Portions copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.janusgraph.diskstorage.tupl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockUpgradeRule;
import org.cojen.tupl.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.diskstorage.util.DirectoryUtil;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * The JanusGraph manager for the Tupl storage backend. Tracks implemented
 * features and implements mutateMany.
 * @author Alexander Patrikalakis
 *
 */
public class TuplStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {
    public static final String CANT_OVERRIDE_LOCK_MODE_WITHOUT_TX_OR_WHEN_BATCH_LOADING =
        "You cannot override the Tupl lock mode when transactions are turned off or you are " +
        "batch loading. The lock mode will always be UNSAFE.";
    public static final String CANNOT_OVERRIDE_DURABILITY_MODE_WHEN_BATCH_LOADING =
        "You cannot override the Tupl durability mode when you are batch loading. " +
        "The durability mode will always be NO_REDO.";

    private static final Logger log = LoggerFactory.getLogger(TuplStoreManager.class);

    public static final ConfigNamespace TUPL_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "tupl",
        "Configuration for the Tupl Storage Backend for Titan.");
    public static final ConfigNamespace TUPL_LOCK_NS =
            new ConfigNamespace(TUPL_NS, "lock",
            "Configuration for the locking aspect of the Tupl Storage Backend for Titan.");
    public static final ConfigNamespace TUPL_CHECKPOINT_NS =
            new ConfigNamespace(TUPL_NS, "checkpoint",
            "Configuration for the checkpointing aspect of the Tupl Storage Backend for Titan.");
    public static final ConfigOption<Boolean> TUPL_MAP_DATA_FILES = new ConfigOption<Boolean>(TUPL_NS,
            "map-data-files",
            "Enable memory mapping of the data files. Entire graph needs to fit in memory.",
            ConfigOption.Type.MASKABLE, Boolean.FALSE);
    public static final ConfigOption<Long> TUPL_MIN_CACHE_SIZE = new ConfigOption<Long>(TUPL_NS,
            "min-cache-size", "The tupl minimum cache size (bytes). Must be at least 5 pages long.",
            ConfigOption.Type.MASKABLE, Long.valueOf(100_000));
    public static final ConfigOption<Long> TUPL_MAX_CACHE_SIZE = new ConfigOption<Long>(TUPL_NS,
            "max-cache-size", "The tupl maximum cache size (bytes). Must be greater than the min cache size.",
            ConfigOption.Type.MASKABLE, Long.valueOf(1_000_000_000));
    public static final ConfigOption<Long> TUPL_SECONDARY_CACHE_SIZE = new ConfigOption<Long>(TUPL_NS,
            "secondary-cache-size", "The tupl secondary cache size (bytes). Off by default.",
            ConfigOption.Type.MASKABLE, Long.valueOf(0));
    public static final ConfigOption<String> TUPL_DURABILITY_MODE = new ConfigOption<String>(TUPL_NS,
            "durability-mode",
            "Default transaction durability mode.",
            ConfigOption.Type.MASKABLE, DurabilityMode.SYNC.name());
    /**
     * REPEATABLE_READ not compatible
     * UPGRADABLE_READ fails transactions too soon (on the concurrent modification, not the commit operation)
     * this may be desirable depending on the situation.
     *
     */
    public static final ConfigOption<String> TUPL_LOCK_MODE = new ConfigOption<String>(TUPL_LOCK_NS,
            "mode",
            "Default lock mode. READ_UNCOMMITTED is needed to pass most Titan KV and KCV tests.",
            ConfigOption.Type.MASKABLE, LockMode.READ_UNCOMMITTED.name());
    public static final ConfigOption<String> TUPL_LOCK_UPGRADE_RULE = new ConfigOption<String>(TUPL_LOCK_NS,
            "upgrade-rule",
            "Default lock upgrade rule.",
            ConfigOption.Type.MASKABLE, LockUpgradeRule.STRICT.name());
    public static final ConfigOption<Long> TUPL_LOCK_TIMEOUT = new ConfigOption<Long>(TUPL_LOCK_NS,
            "timeout", "The lock timeout (milliseconds).",
            ConfigOption.Type.MASKABLE, Long.valueOf(60_000));
    public static final ConfigOption<Boolean> TUPL_SYNC_WRITES = new ConfigOption<Boolean>(TUPL_NS,
            "sync-writes",
            "Set true to ensure all writes to the main database file are immediately durable, although " +
            "not checkpointed. This option typically reduces overall performance, but checkpoints " +
            "complete more quickly. As a result, the main database file requires less pre-allocated " +
            "pages and is smaller.",
            ConfigOption.Type.MASKABLE, Boolean.FALSE);
    public static final ConfigOption<Boolean> TUPL_DIRECT_PAGE_ACCESS = new ConfigOption<Boolean>(TUPL_NS,
            "direct-page-access",
            "Set true to allocate all pages off the Java heap, offering increased performance and " +
            "reduced garbage collection activity.",
            ConfigOption.Type.MASKABLE, Boolean.TRUE);
    public static final ConfigOption<Integer> TUPL_PAGE_SIZE = new ConfigOption<Integer>(TUPL_NS,
            "page-size", "The page size in bytes.",
            ConfigOption.Type.MASKABLE, Integer.valueOf(4096));
    public static final ConfigOption<Long> TUPL_CHECKPOINT_RATE = new ConfigOption<Long>(TUPL_CHECKPOINT_NS,
            "rate", "The checkpoint rate in milliseconds. Set to a negative number to disable automatic checkpoints.",
            ConfigOption.Type.MASKABLE, Long.valueOf(1000));
    public static final ConfigOption<Long> TUPL_CHECKPOINT_DELAY_THRESHOLD = new ConfigOption<Long>(TUPL_CHECKPOINT_NS,
            "delay-threshold", "The checkpoint delay threshold in milliseconds (infinite if negative). This delay takes precedence over the size threshold. Set to zero for non-transactional operations.",
            ConfigOption.Type.MASKABLE, Long.valueOf(60000));
    public static final ConfigOption<Long> TUPL_CHECKPOINT_SIZE_THRESHOLD = new ConfigOption<Long>(TUPL_CHECKPOINT_NS,
            "size-threshold", "The checkpoint size threshold in bytes. Set to zero for non-transactional operations.",
            ConfigOption.Type.MASKABLE, Long.valueOf(1024*1024*1024));
    //TODO turn off checkpointing and cleaner when batch loading

    /**
     * Had to subclass StoreFeatures instead of using StandardStoreFeatures.Builder
     * because the builder does not allow setting supportsPersistence=false
     * @author Alexander Patrikalakis
     *
     */
    public class TuplStoreFeatures implements StoreFeatures {
        private final boolean transactional;
        private final boolean persistent;
        public TuplStoreFeatures(boolean transactional, boolean persistent) {
            this.transactional = transactional;
            this.persistent = persistent;
        }

        @Override public boolean hasScan()                              { return true; }
        @Override public boolean hasUnorderedScan()                     { return false; }
        @Override public boolean hasOrderedScan()                       { return true; }
        @Override public boolean hasMultiQuery()                        { return false; }
        @Override public boolean hasLocking()                           { return true; }

        //Does this storage backend supports batch mutations via mutateMany? Yes
        //TODO mutateMany is implemented, but is it a batch operation? Should this be false?
        @Override public boolean hasBatchMutation()                     { return true; }
        @Override public boolean isKeyOrdered()                         { return true; }
        @Override public boolean isDistributed()                        { return false; }

        //storage.transactions=false will turn off transactions here, but needs to be propagated
        //into the beginTransaction() method of the TuplStoreManager
        @Override public boolean hasTxIsolation()                       { return transactional; }
        @Override public boolean hasLocalKeyPartition()                 { return false; }

        //do not need to do anything special for key consistent operations,
        //tupl transactions support them out of the box
        @Override public boolean isKeyConsistent()                      { return true; }
        @Override public boolean hasTimestamps()                        { return false; }
        @Override public TimestampProviders getPreferredTimestamps()    { return null; }
        @Override public boolean hasCellTTL()                           { return false; }
        @Override public boolean hasStoreTTL()                          { return false; }

        //TODO Tupl supports encryption. this could be enabled in the future
        @Override public boolean hasVisibility()                        { return false; }
        @Override public boolean supportsPersistence()                  { return persistent; }
        @Override public Configuration getKeyConsistentTxConfig()       { return buildGraphConfig(); }
        @Override public Configuration getLocalKeyConsistentTxConfig()  { return null; }

        /**
         * This transaction configuration should disable transaction isolation as it
         * will only be used for reads that are rolled back.
         * the configurations, in decreasing order of precedence
         * scanTxConfig overrides graphConfiguration which overrides defaultValue
         */
        @Override public Configuration getScanTxConfig() {
            return buildGraphConfig().set(TUPL_LOCK_MODE, LockMode.UNSAFE.name());
        }
        @Override public boolean supportsInterruption()                 { return false; }

        @Override public boolean hasOptimisticLocking() { return true; }
    }

    private final Map<String, TuplKeyValueStore> stores;
    private final String prefix = "graph";
    private final StoreFeatures features;
    private final DatabaseConfig config;
    //clearStorage() replaces the initial db from ctor with a new instance
    private Database database;
    private final File directory;
    private final File prefixFile;
    private final File lockFile;

    public TuplStoreManager(Configuration storageConfig) throws BackendException {
        //sets transactional, batchLoading, directory
        super(storageConfig);
        final boolean persistent = null != prefix;
        if(persistent) {
            if (!storageConfig.has(GraphDatabaseConfiguration.STORAGE_DIRECTORY)) {
                directory = new File(System.getProperty("user.dir"));
            } else {
                directory = DirectoryUtil.getOrCreateDataDirectory(storageConfig.get(GraphDatabaseConfiguration.STORAGE_DIRECTORY));
            }
            prefixFile = new File(directory, prefix);
            lockFile = new File(prefixFile.getAbsolutePath() + ".lock");
        } else {
            directory = null;
            prefixFile = null;
            lockFile = null;
        }

        final boolean mapDataFiles = storageConfig.get(TUPL_MAP_DATA_FILES);
        final long minCacheSize = storageConfig.get(TUPL_MIN_CACHE_SIZE);
        final long maxCacheSize = storageConfig.get(TUPL_MAX_CACHE_SIZE);
        final long secondaryCacheSize = storageConfig.get(TUPL_SECONDARY_CACHE_SIZE);
        final DurabilityMode durabilityMode =
                DurabilityMode.valueOf(storageConfig.get(TUPL_DURABILITY_MODE));
        final LockUpgradeRule lockUpgradeRule = LockUpgradeRule.STRICT;
        final long lockTimeout = storageConfig.get(TUPL_LOCK_TIMEOUT);
        final boolean syncWrites = storageConfig.get(TUPL_SYNC_WRITES);
        final int pageSize = storageConfig.get(TUPL_PAGE_SIZE);
        final boolean directPageAccess = storageConfig.get(TUPL_DIRECT_PAGE_ACCESS);
        final long checkpointRate = storageConfig.get(TUPL_CHECKPOINT_RATE);
        final long checkpointDelayThreshold = storageConfig.get(TUPL_CHECKPOINT_DELAY_THRESHOLD);
        final long checkpointSizeThreshold = storageConfig.get(TUPL_CHECKPOINT_SIZE_THRESHOLD);

        features = new TuplStoreFeatures(transactional, persistent);
        stores = new HashMap<>();

        config = new DatabaseConfig().baseFilePath(persistent ? prefixFile.getAbsolutePath() : null)
                                     .mapDataFiles(mapDataFiles)
                                     .minCacheSize(minCacheSize)
                                     .maxCacheSize(maxCacheSize)
                                     .secondaryCacheSize(secondaryCacheSize)
                                     .durabilityMode(durabilityMode)
                                     .lockUpgradeRule(lockUpgradeRule)
                                     .lockTimeout(lockTimeout, TimeUnit.MILLISECONDS)
                                     .syncWrites(syncWrites)
                                     .pageSize(pageSize)
                                     .directPageAccess(directPageAccess)
                                     .checkpointRate(checkpointRate, TimeUnit.MILLISECONDS)
                                     .checkpointDelayThreshold(checkpointDelayThreshold, TimeUnit.MILLISECONDS)
                                     .checkpointSizeThreshold(checkpointSizeThreshold);

        try {
            database = Database.open(config);
        } catch(IOException e) {
            throw new PermanentBackendException("unable to open the database", e);
        }
    }

    private ModifiableConfiguration buildGraphConfig() {
        return GraphDatabaseConfiguration.buildGraphConfiguration();
    }

    public TuplStoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        final Configuration effectiveCfg = new MergedConfiguration(
            config.getCustomOptions() /*overrides, defined in individual titan transaction configs*/,
            getStorageConfig() /*defaults, in superclass AbstractStoreManager, passed into ctor*/);

        //configure the durability mode
        if(batchLoading && effectiveCfg.has(TUPL_DURABILITY_MODE)) {
            throw new PermanentBackendException(CANNOT_OVERRIDE_DURABILITY_MODE_WHEN_BATCH_LOADING);
        }
        final DurabilityMode effectiveDurabilityMode = batchLoading ? DurabilityMode.NO_REDO :
            DurabilityMode.valueOf(effectiveCfg.get(TUPL_DURABILITY_MODE));

        //configure the lock mode
        if((!transactional || batchLoading) && effectiveCfg.has(TUPL_LOCK_MODE)) {
            throw new PermanentBackendException(CANT_OVERRIDE_LOCK_MODE_WITHOUT_TX_OR_WHEN_BATCH_LOADING);
        }
        final LockMode effectiveLockMode =
            (transactional && !batchLoading) ? LockMode.valueOf(effectiveCfg.get(TUPL_LOCK_MODE))
                                             : LockMode.UNSAFE;

        final Transaction txn = database.newTransaction(effectiveDurabilityMode);
        txn.lockMode(effectiveLockMode);

        return new TuplStoreTransaction(config, txn, database);
    }

    public void unregisterStore(TuplKeyValueStore db) throws PermanentBackendException {
        if (!stores.containsKey(db.getName())) {
            return;
        }
        stores.remove(db.getName());
    }

    public void close() throws BackendException {
        try {
            database.shutdown();
            if(null != lockFile) {
                lockFile.delete();
            }
        } catch (IOException e) {
            throw new PermanentBackendException("Could not close Tupl database", e);
        }
    }

    public void clearStorage() throws BackendException {
        try {
            close();
            database = Database.destroy(config);
        } catch (IOException e) {
            throw new PermanentBackendException("unable to clear storage", e);
        }
    }

    public StoreFeatures getFeatures() {
        return features;
    }

    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        //All storage is local with Tupl, at least now
        //if tupl replication is used on multiple hosts, it is possible that this may change
        throw new UnsupportedOperationException();
    }

    public TuplKeyValueStore openDatabase(String name, Container metaData /*unused*/) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }

        final Index dbindex;
        try {
            dbindex = database.openIndex(name);
        } catch (IOException e) {
            throw new PermanentBackendException("unable to open " + name, e);
        }

        final TuplKeyValueStore store = new TuplKeyValueStore(name, dbindex, this);
        stores.put(name, store);
        return store;
    }

    public TuplKeyValueStore openDatabase(String name) throws BackendException {
        return this.openDatabase(name, null /*metaData*/);
    }

    @VisibleForTesting
    void mutateOne(Map.Entry<String, KVMutation> mapping, StoreTransaction txh) throws BackendException {
        final String name = mapping.getKey();
        final KVMutation changes = mapping.getValue();
        final TuplKeyValueStore store = this.openDatabase(name);

        log.debug("Mutating {}", name);

        if (changes.hasAdditions()) {
            for (KeyValueEntry entry : changes.getAdditions()) {
                store.insert(entry.getKey(), entry.getValue(), txh);
                log.trace("Insertion on {}: {}", name, entry);
            }
        }
        if (changes.hasDeletions()) {
            for (StaticBuffer del : changes.getDeletions()) {
                store.delete(del, txh);
                log.trace("Deletion on {}: {}", name, del);
            }
        }
    }

    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        TuplStoreTransaction tx = TuplStoreTransaction.getTx(txh);
        Map<String, KVMutation> storesWithChanges = mutations.entrySet().stream()
                .filter(mut -> mut.getValue().hasAdditions() || mut.getValue().hasDeletions())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for(Map.Entry<String, KVMutation> entry : storesWithChanges.entrySet()) {
            this.mutateOne(entry, tx);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + " : " +
                (prefixFile ==  null ? "inmemory" : (prefixFile.toString() + "*"));
    }
}
