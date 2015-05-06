package com.thinkaurelius.titan.diskstorage.locking.consistentkey;


import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.MergedConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.LockerProvider;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpectedValueCheckingStoreManager extends KCVSManagerProxy {

    private final String lockStoreSuffix;
    private final LockerProvider lockerProvider;
    private final Duration maxReadTime;
    private final StoreFeatures storeFeatures;

    private final Map<String,ExpectedValueCheckingStore> stores;

    private static final Logger log =
            LoggerFactory.getLogger(ExpectedValueCheckingStoreManager.class);

    public ExpectedValueCheckingStoreManager(KeyColumnValueStoreManager storeManager, String lockStoreSuffix,
                                             LockerProvider lockerProvider, Duration maxReadTime) {
        super(storeManager);
        this.lockStoreSuffix = lockStoreSuffix;
        this.lockerProvider = lockerProvider;
        this.maxReadTime = maxReadTime;
        this.storeFeatures = new StandardStoreFeatures.Builder(storeManager.getFeatures()).locking(true).build();
        this.stores = new HashMap<String,ExpectedValueCheckingStore>(6);
    }

    @Override
    public synchronized KeyColumnValueStore openDatabase(String name) throws BackendException {
        if (stores.containsKey(name)) return stores.get(name);
        KeyColumnValueStore store = manager.openDatabase(name);
        final String lockerName = store.getName() + lockStoreSuffix;
        ExpectedValueCheckingStore wrappedStore = new ExpectedValueCheckingStore(store, lockerProvider.getLocker(lockerName));
        stores.put(name,wrappedStore);
        return wrappedStore;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        ExpectedValueCheckingTransaction etx = (ExpectedValueCheckingTransaction)txh;
        boolean hasAtLeastOneLock = etx.prepareForMutations();
        if (hasAtLeastOneLock) {
            // Force all mutations on this transaction to use strong consistency
            log.debug("Transaction {} holds one or more locks: writing using consistent transaction {} due to held locks", etx, etx.getConsistentTx());
            manager.mutateMany(mutations, etx.getConsistentTx());
        } else {
            log.debug("Transaction {} holds no locks: writing mutations using store transaction {}", etx, etx.getInconsistentTx());
            manager.mutateMany(mutations, etx.getInconsistentTx());
        }
    }

    @Override
    public ExpectedValueCheckingTransaction beginTransaction(BaseTransactionConfig configuration) throws BackendException {
        // Get a transaction without any guarantees about strong consistency
        StoreTransaction inconsistentTx = manager.beginTransaction(configuration);

        // Get a transaction that provides global strong consistency
        Configuration customOptions = new MergedConfiguration(storeFeatures.getKeyConsistentTxConfig(), configuration.getCustomOptions());
        BaseTransactionConfig consistentTxCfg = new StandardBaseTransactionConfig.Builder(configuration)
                .customOptions(customOptions).build();
        StoreTransaction strongConsistentTx = manager.beginTransaction(consistentTxCfg);

        // Return a wrapper around both the inconsistent and consistent store transactions
        ExpectedValueCheckingTransaction wrappedTx = new ExpectedValueCheckingTransaction(inconsistentTx, strongConsistentTx, maxReadTime);
        return wrappedTx;
    }

    @Override
    public StoreFeatures getFeatures() {
        return storeFeatures;
    }

}
