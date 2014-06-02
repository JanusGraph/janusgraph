package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.MergedConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.LockerProvider;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpectedValueCheckingStoreManager implements KeyColumnValueStoreManager {

    private final KeyColumnValueStoreManager storeManager;
    private final String lockStoreSuffix;
    private final LockerProvider lockerProvider;
    private final Duration maxReadTime;
    private final StoreFeatures storeFeatures;

    private final Map<String,ExpectedValueCheckingStore> stores;

    public ExpectedValueCheckingStoreManager(KeyColumnValueStoreManager storeManager, String lockStoreSuffix,
                                             LockerProvider lockerProvider, Duration maxReadTime) {
        this.storeManager = storeManager;
        this.lockStoreSuffix = lockStoreSuffix;
        this.lockerProvider = lockerProvider;
        this.maxReadTime = maxReadTime;
        this.storeFeatures = storeManager.getFeatures();
        this.stores = new HashMap<String,ExpectedValueCheckingStore>(6);
    }

    @Override
    public synchronized KeyColumnValueStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name)) return stores.get(name);
        KeyColumnValueStore store = storeManager.openDatabase(name);
        final String lockerName = store.getName() + lockStoreSuffix;
        ExpectedValueCheckingStore wrappedStore = new ExpectedValueCheckingStore(store, lockerProvider.getLocker(lockerName));
        stores.put(name,wrappedStore);
        return wrappedStore;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        for (String store : mutations.keySet()) {
            stores.get(store).verifyLocksOnMutations(txh);
        }
        storeManager.mutateMany(mutations,ExpectedValueCheckingStore.getDataTx(txh));
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig configuration) throws StorageException {
        StoreTransaction tx = storeManager.beginTransaction(configuration);

        Configuration customOptions = new MergedConfiguration(storeFeatures.getKeyConsistentTxConfig(), configuration.getCustomOptions());
        BaseTransactionConfig consistentTxCfg = new StandardBaseTransactionConfig.Builder(configuration)
                .customOptions(customOptions)
                .build();
        StoreTransaction consistentTx = storeManager.beginTransaction(consistentTxCfg);
        StoreTransaction wrappedTx = new ExpectedValueCheckingTransaction(tx, consistentTx, maxReadTime);
        return wrappedTx;
    }

    @Override
    public void close() throws StorageException {
        storeManager.close();
    }

    @Override
    public void clearStorage() throws StorageException {
        storeManager.clearStorage();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        return storeManager.getLocalKeyPartition();
    }

    @Override
    public StoreFeatures getFeatures() {
        StoreFeatures features = new StandardStoreFeatures.Builder(storeFeatures).locking(true).build();
        return features;
    }

    @Override
    public String getName() {
        return storeManager.getName();
    }
}
