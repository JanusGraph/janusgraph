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

package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps a {@link OrderedKeyValueStoreManager} and exposes it as a {@link KeyColumnValueStoreManager}.
 * <p>
 * An optional mapping of key-length can be defined if it is known that the {@link KeyColumnValueStore} of a given
 * name has a static key length. See {@link OrderedKeyValueStoreAdapter} for more information.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class OrderedKeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {


    private final OrderedKeyValueStoreManager manager;

    private final Map<String, Integer> keyLengths;

    private final Map<String, OrderedKeyValueStoreAdapter> stores;

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager) {
        this(manager, new HashMap<>());
    }

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager, Map<String, Integer> keyLengths) {
        Preconditions.checkArgument(manager.getFeatures().isKeyOrdered(), "Expected backing store to be ordered: %s", manager);
        this.manager = manager;
        if (keyLengths != null && !keyLengths.isEmpty()) {
            this.keyLengths = Collections.unmodifiableMap(new HashMap<>(keyLengths));
        } else {
            this.keyLengths = Collections.emptyMap();
        }
        this.stores = new HashMap<>();
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
    public boolean exists() throws BackendException {
        return manager.exists();
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
        final Map<String, KVMutation> converted = new HashMap<>(mutations.size());
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeEntry : mutations.entrySet()) {
            OrderedKeyValueStoreAdapter store = openDatabase(storeEntry.getKey());
            Preconditions.checkNotNull(store);

            KVMutation mut = new KVMutation();
            for (Map.Entry<StaticBuffer, KCVMutation> entry : storeEntry.getValue().entrySet()) {
                StaticBuffer key = entry.getKey();
                KCVMutation mutation = entry.getValue();
                if (mutation.hasAdditions()) {
                    for (Entry addition : mutation.getAdditions()) {
                        KeyValueEntry concatenate = store.concatenate(key, addition);
                        concatenate.setTTL((Integer) addition.getMetaData().get(EntryMetaData.TTL));
                        mut.addition(concatenate);
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

    private static OrderedKeyValueStoreAdapter wrapKeyValueStore(OrderedKeyValueStore store, Map<String, Integer> keyLengths) {
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
