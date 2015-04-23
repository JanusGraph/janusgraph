package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StoreMetaData;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps a {@link OrderedKeyValueStoreManager} and exposes it as a {@link KeyColumnValueStoreManager}.
 * <p/>
 * An optional mapping of key-length can be defined if it is known that the {@link KeyColumnValueStore} of a given
 * name has a static key length. See {@link OrderedKeyValueStoreAdapter} for more information.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class OrderedKeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {


    private final OrderedKeyValueStoreManager manager;

    private final ImmutableMap<String, Integer> keyLengths;

    private final Map<String, OrderedKeyValueStoreAdapter> stores;

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager) {
        this(manager, new HashMap<String, Integer>());
    }

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager, Map<String, Integer> keyLengths) {
        Preconditions.checkArgument(manager.getFeatures().isKeyOrdered(), "Expected backing store to be ordered: %s", manager);
        this.manager = manager;
        ImmutableMap.Builder<String, Integer> mb = ImmutableMap.builder();
        if (keyLengths != null && !keyLengths.isEmpty()) mb.putAll(keyLengths);
        this.keyLengths = mb.build();
        this.stores = new HashMap<String, OrderedKeyValueStoreAdapter>();
    }

    @Override
    public StoreFeatures getFeatures() {
        return manager.getFeatures();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return manager.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        manager.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        manager.clearStorage();
    }

    @Override
    public synchronized OrderedKeyValueStoreAdapter openDatabase(String name) throws BackendException {
        return openDatabase(name, StoreMetaData.EMPTY);
    }

    @Override
    public synchronized OrderedKeyValueStoreAdapter openDatabase(String name, StoreMetaData.Container metaData)
            throws BackendException {
        if (!stores.containsKey(name) || stores.get(name).isClosed()) {
            OrderedKeyValueStoreAdapter store = wrapKeyValueStore(manager.openDatabase(name), keyLengths);
            stores.put(name, store);
        }
        return stores.get(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Map<String, KVMutation> converted = new HashMap<String, KVMutation>(mutations.size());
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeEntry : mutations.entrySet()) {
            OrderedKeyValueStoreAdapter store = openDatabase(storeEntry.getKey());
            Preconditions.checkNotNull(store);

            KVMutation mut = new KVMutation();
            for (Map.Entry<StaticBuffer, KCVMutation> entry : storeEntry.getValue().entrySet()) {
                StaticBuffer key = entry.getKey();
                KCVMutation mutation = entry.getValue();
                if (mutation.hasAdditions()) {
                    for (Entry addition : mutation.getAdditions()) {
                        mut.addition(store.concatenate(key,addition));
                    }
                }

                if (mutation.hasDeletions()) {
                    for (StaticBuffer del : mutation.getDeletions()) {
                        mut.deletion(store.concatenate(key, del));
                    }
                }
            }
            converted.put(storeEntry.getKey(), mut);
        }
        manager.mutateMany(converted, txh);
    }

    private static final OrderedKeyValueStoreAdapter wrapKeyValueStore(OrderedKeyValueStore store, Map<String, Integer> keyLengths) {
        String name = store.getName();
        if (keyLengths.containsKey(name)) {
            int keyLength = keyLengths.get(name);
            Preconditions.checkArgument(keyLength > 0);
            return new OrderedKeyValueStoreAdapter(store, keyLength);
        } else {
            return new OrderedKeyValueStoreAdapter(store);
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return manager.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return manager.getName();
    }
}
