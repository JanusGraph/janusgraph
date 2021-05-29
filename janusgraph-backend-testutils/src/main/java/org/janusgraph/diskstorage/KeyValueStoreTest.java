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

package org.janusgraph.diskstorage;


import com.google.common.collect.Lists;
import org.janusgraph.JanusGraphBaseStoreFeaturesTest;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVUtil;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.testutil.FeatureFlag;
import org.janusgraph.testutil.JanusGraphFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class KeyValueStoreTest extends AbstractKCVSTest implements JanusGraphBaseStoreFeaturesTest {

    private final Logger log = LoggerFactory.getLogger(KeyValueStoreTest.class);

    private final int numKeys = 2000;


    protected OrderedKeyValueStoreManager manager;
    protected StoreTransaction tx;
    protected OrderedKeyValueStore store;

    @BeforeEach
    public void setUp() throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        open();
    }

    public void open() throws BackendException {
        manager = openStorageManager();
        String storeName = "testStore1";
        store = manager.openDatabase(storeName);
        tx = manager.beginTransaction(getTxConfig());
    }

    public abstract OrderedKeyValueStoreManager openStorageManager() throws BackendException;

    @Override
    public StoreFeatures getStoreFeatures(){
        return manager.getFeatures();
    }

    @AfterEach
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws BackendException {
        if (tx != null) tx.commit();
        store.close();
        manager.close();
    }

    public void clopen() throws BackendException {
        close();
        open();
    }

    @Test
    public void createDatabase() {
        //Just setup and shutdown
    }


    public String[] generateValues() {
        return KeyValueStoreUtil.generateData(numKeys);
    }

    public void loadValues(String[] values) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            store.insert(KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(values[i]), tx, null);
        }
    }

    public Set<Integer> deleteValues(int start, int every) throws BackendException {
        final Set<Integer> removed = new HashSet<>();
        for (int i = start; i < numKeys; i = i + every) {
            removed.add(i);
            store.delete(KeyValueStoreUtil.getBuffer(i), tx);
        }
        return removed;
    }

    public void checkValueExistence(String[] values) throws BackendException {
        checkValueExistence(values, new HashSet<>());
    }

    public void checkValueExistence(String[] values, Set<Integer> removed) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            boolean result = store.containsKey(KeyValueStoreUtil.getBuffer(i), tx);
            if (removed.contains(i)) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }
    }

    public void checkValues(String[] values) throws BackendException {
        checkValues(values, new HashSet<>());
    }

    public void checkValues(String[] values, Set<Integer> removed) throws BackendException {
        //1. Check one-by-one
        for (int i = 0; i < numKeys; i++) {
            StaticBuffer result = store.get(KeyValueStoreUtil.getBuffer(i), tx);
            if (removed.contains(i)) {
                assertNull(result);
            } else {
                assertEquals(values[i], KeyValueStoreUtil.getString(result));
            }
        }
        //2. Check all at once (if supported)
        if (manager.getFeatures().hasMultiQuery()) {
            List<KVQuery> queries = Lists.newArrayList();
            for (int i = 0; i < numKeys; i++) {
                StaticBuffer key = KeyValueStoreUtil.getBuffer(i);
                queries.add(new KVQuery(key, BufferUtil.nextBiggerBuffer(key),2));
            }
            Map<KVQuery,RecordIterator<KeyValueEntry>> results = store.getSlices(queries,tx);
            for (int i = 0; i < numKeys; i++) {
                RecordIterator<KeyValueEntry> result = results.get(queries.get(i));
                assertNotNull(result);
                StaticBuffer value;
                if (result.hasNext()) {
                    value = result.next().getValue();
                    assertFalse(result.hasNext());
                } else value=null;
                if (removed.contains(i)) {
                    assertNull(value);
                } else {
                    assertEquals(values[i], KeyValueStoreUtil.getString(value));
                }
            }
        }
    }

    @Test
    public void storeAndRetrieve() throws BackendException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);

        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void storeAndRetrieveWithClosing() throws BackendException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void deletionTest1() throws BackendException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        Set<Integer> deleted = deleteValues(0, 10);
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void deletionTest2() throws BackendException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<Integer> deleted = deleteValues(0, 10);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    @FeatureFlag(feature = JanusGraphFeature.Scan)
    public void scanTest() throws BackendException {
        String[] values = generateValues();
        loadValues(values);
        RecordIterator<KeyValueEntry> iterator0 = getAllData(tx);
        assertEquals(numKeys, KeyValueStoreUtil.count(iterator0));
        clopen();
        RecordIterator<KeyValueEntry> iterator1 = getAllData(tx);
        RecordIterator<KeyValueEntry> iterator2 = getAllData(tx);

        // The idea is to open an iterator without using it
        // to make sure that closing a transaction will clean it up.
        // (important for BerkeleyJE where leaving cursors open causes exceptions)
        @SuppressWarnings("unused")
        RecordIterator<KeyValueEntry> iterator3 = getAllData(tx);
        assertEquals(numKeys, KeyValueStoreUtil.count(iterator1));
        assertEquals(numKeys, KeyValueStoreUtil.count(iterator2));
    }

    private RecordIterator<KeyValueEntry> getAllData(StoreTransaction tx) throws BackendException {
        return store.getSlice(new KVQuery(BackendTransaction.EDGESTORE_MIN_KEY, BackendTransaction.EDGESTORE_MAX_KEY), tx);
    }


    public void checkSlice(String[] values, Set<Integer> removed, int start, int end, int limit) throws BackendException {
        EntryList entries;
        if (limit <= 0)
            entries = KVUtil.getSlice(store, KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end), tx);
        else
            entries = KVUtil.getSlice(store, KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end), limit, tx);

        int pos = 0;
        for (int i = start; i < end; i++) {
            if (removed.contains(i)) continue;
            if (pos < limit) {
                Entry entry = entries.get(pos);
                int id = KeyValueStoreUtil.getID(entry.getColumn());
                String str = KeyValueStoreUtil.getString(entry.getValueAs(StaticBuffer.STATIC_FACTORY));
                assertEquals(i, id);
                assertEquals(values[i], str);
            }
            pos++;
        }
        if (limit > 0 && pos >= limit) assertEquals(limit, entries.size());
        else {
            assertNotNull(entries);
            assertEquals(pos, entries.size());
        }
    }

    @Test
    public void intervalTest1() throws BackendException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<Integer> deleted = deleteValues(0, 10);
        clopen();
        checkSlice(values, deleted, 5, 25, -1);
        checkSlice(values, deleted, 5, 250, 10);
        checkSlice(values, deleted, 500, 1250, -1);
        checkSlice(values, deleted, 500, 1250, 1000);
        checkSlice(values, deleted, 500, 1250, 100);
        checkSlice(values, deleted, 50, 20, 10);
        checkSlice(values, deleted, 50, 20, -1);

    }


}
