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

package org.janusgraph.diskstorage.berkeleyje;


import com.google.common.base.Preconditions;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.LocalStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;

@PreInitializeConfigOptions
public class BerkeleyJEStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEStoreManager.class);

    public static final ConfigNamespace BERKELEY_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "berkeleyje", "BerkeleyDB JE configuration options");

    public static final ConfigOption<Integer> JVM_CACHE =
            new ConfigOption<>(BERKELEY_NS, "cache-percentage",
            "Percentage of JVM heap reserved for BerkeleyJE's cache",
            ConfigOption.Type.MASKABLE, 65, ConfigOption.positiveInt());

    public static final ConfigOption<Boolean> SHARED_CACHE =
        new ConfigOption<>(BERKELEY_NS, "shared-cache",
            "If true, the shared cache is used for all graph instances",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String> LOCK_MODE =
            new ConfigOption<>(BERKELEY_NS, "lock-mode",
            "The BDB record lock mode used for read operations",
            ConfigOption.Type.MASKABLE, String.class, LockMode.DEFAULT.toString(), disallowEmpty(String.class));

    public static final ConfigOption<String> CACHE_MODE =
        new ConfigOption<>(BERKELEY_NS, "cache-mode",
            "Modes that can be specified for control over caching of records in the JE in-memory cache",
            ConfigOption.Type.MASKABLE,  String.class, CacheMode.DEFAULT.toString(), disallowEmpty(String.class));

    public static final ConfigOption<String> ISOLATION_LEVEL =
            new ConfigOption<>(BERKELEY_NS, "isolation-level",
            "The isolation level used by transactions",
            ConfigOption.Type.MASKABLE,  String.class,
            IsolationLevel.REPEATABLE_READ.toString(), disallowEmpty(String.class));

    private final Map<String, BerkeleyJEKeyValueStore> stores;

    protected Environment environment;
    protected final StoreFeatures features;

    public BerkeleyJEStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new HashMap<>();

        int cachePercentage = configuration.get(JVM_CACHE);
        boolean sharedCache = configuration.get(SHARED_CACHE);
        CacheMode cacheMode = ConfigOption.getEnumValue(configuration.get(CACHE_MODE), CacheMode.class);
        initialize(cachePercentage, sharedCache, cacheMode);

        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .scanTxConfig(GraphDatabaseConfiguration.buildGraphConfiguration()
                            .set(ISOLATION_LEVEL, IsolationLevel.READ_UNCOMMITTED.toString())
                    )
                    .supportsInterruption(false)
                    .cellTTL(true)
                    .optimisticLocking(false)
                    .build();
    }

    private void initialize(int cachePercent, final boolean sharedCache, final CacheMode cacheMode) throws BackendException {
        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(transactional);
            envConfig.setCachePercent(cachePercent);
            envConfig.setSharedCache(sharedCache);
            envConfig.setCacheMode(cacheMode);

            if (batchLoading) {
                envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
                envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
            }

            //Open the environment
            environment = new Environment(directory, envConfig);

        } catch (DatabaseException e) {
            throw new PermanentBackendException("Error during BerkeleyJE initialization: ", e);
        }

    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BerkeleyJETx beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        try {
            Transaction tx = null;

            Configuration effectiveCfg =
                    new MergedConfiguration(txCfg.getCustomOptions(), getStorageConfig());

            if (transactional) {
                TransactionConfig txnConfig = new TransactionConfig();
                ConfigOption.getEnumValue(effectiveCfg.get(ISOLATION_LEVEL), IsolationLevel.class).configure(txnConfig);
                tx = environment.beginTransaction(null, txnConfig);
            } else {
                if (txCfg instanceof TransactionConfiguration) {
                    if (!((TransactionConfiguration) txCfg).isSingleThreaded()) {
                        // Non-transactional cursors can't shared between threads, more info ThreadLocker.checkState
                        throw new PermanentBackendException("BerkeleyJE does not support non-transactional for multi threaded tx");
                    }
                }
            }
            BerkeleyJETx btx =
                new BerkeleyJETx(
                    tx,
                    ConfigOption.getEnumValue(effectiveCfg.get(LOCK_MODE), LockMode.class),
                    ConfigOption.getEnumValue(effectiveCfg.get(CACHE_MODE), CacheMode.class),
                    txCfg);

            if (log.isTraceEnabled()) {
                log.trace("Berkeley tx created", new TransactionBegin(btx.toString()));
            }

            return btx;
        } catch (DatabaseException e) {
            throw new PermanentBackendException("Could not start BerkeleyJE transaction", e);
        }
    }

    @Override
    public BerkeleyJEKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(false);
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(transactional);
            dbConfig.setKeyPrefixing(true);

            if (batchLoading) {
                dbConfig.setDeferredWrite(true);
            }

            Database db = environment.openDatabase(null, name, dbConfig);

            log.debug("Opened database {}", name);

            BerkeleyJEKeyValueStore store = new BerkeleyJEKeyValueStore(name, db, this);
            stores.put(name, store);
            return store;
        } catch (DatabaseException e) {
            throw new PermanentBackendException("Could not open BerkeleyJE data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String,KVMutation> mutation : mutations.entrySet()) {
            BerkeleyJEKeyValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh, entry.getTtl());
                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(BerkeleyJEKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


    @Override
    public void close() throws BackendException {
        if (environment != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            } catch (InterruptedException e) {
                //Ignore
            }
            try {
                environment.close();
            } catch (DatabaseException e) {
                throw new PermanentBackendException("Could not close BerkeleyJE database", e);
            }
        }

    }

    private static final Transaction NULL_TRANSACTION = null;

    @Override
    public void clearStorage() throws BackendException {
        if (!stores.isEmpty()) {
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet());
        }

        for (final String db : environment.getDatabaseNames()) {
            environment.removeDatabase(NULL_TRANSACTION, db);
            log.debug("Removed database {} (clearStorage)", db);
        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public boolean exists() throws BackendException {
        return !environment.getDatabaseNames().isEmpty();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }


    public enum IsolationLevel {
        READ_UNCOMMITTED {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setReadUncommitted(true);
            }
        }, READ_COMMITTED {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setReadCommitted(true);

            }
        }, REPEATABLE_READ {
            @Override
            void configure(TransactionConfig cfg) {
                // This is the default and has no setter
            }
        }, SERIALIZABLE {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setSerializableIsolation(true);
            }
        };

        abstract void configure(TransactionConfig cfg);
    }

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }
}
