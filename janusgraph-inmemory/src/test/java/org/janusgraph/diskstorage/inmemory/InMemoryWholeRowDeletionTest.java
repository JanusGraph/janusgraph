// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InMemoryWholeRowDeletionTest {

    private static Entry entry(int col, int val) {
        return StaticArrayEntry.of(BufferUtil.getIntBuffer(col), BufferUtil.getIntBuffer(val));
    }

    @Test
    public void featureAdvertised() {
        assertTrue(new InMemoryStoreManager().getFeatures().hasOptimizedWholeRowDeletion(),
            "InMemoryStoreManager should advertise hasOptimizedWholeRowDeletion=true");
    }

    @Test
    public void wholeRowDeletionRemovesAllColumns() throws BackendException {
        InMemoryStoreManager mgr = new InMemoryStoreManager();
        StoreTransaction txh = mgr.beginTransaction(
            StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        KeyColumnValueStore store = mgr.openDatabase("edgestore");
        StaticBuffer key = BufferUtil.getIntBuffer(7);

        // Insert 3 columns
        store.mutate(key, Arrays.asList(entry(1, 10), entry(2, 20), entry(3, 30)),
            KeyColumnValueStore.NO_DELETIONS, txh);

        // Sanity check: 3 columns present
        SliceQuery all = new SliceQuery(BufferUtil.zeroBuffer(4), BufferUtil.oneBuffer(4));
        assertEquals(3, store.getSlice(new KeySliceQuery(key, all), txh).size(),
            "Expected 3 columns before whole-row deletion");

        // Whole-row deletion via mutateMany
        KCVMutation m = new KCVMutation(KeyColumnValueStore.NO_ADDITIONS, KeyColumnValueStore.NO_DELETIONS);
        m.setWholeRowDeletion(true);
        mgr.mutateMany(Collections.singletonMap("edgestore", Collections.singletonMap(key, m)), txh);

        assertEquals(0, store.getSlice(new KeySliceQuery(key, all), txh).size(),
            "Expected 0 columns after whole-row deletion");
    }

    @Test
    public void wholeRowDeletionThenAdditionKeepsOnlyNewColumn() throws BackendException {
        InMemoryStoreManager mgr = new InMemoryStoreManager();
        StoreTransaction txh = mgr.beginTransaction(
            StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        KeyColumnValueStore store = mgr.openDatabase("edgestore");
        StaticBuffer key = BufferUtil.getIntBuffer(7);

        // Insert 3 columns
        store.mutate(key, Arrays.asList(entry(1, 10), entry(2, 20), entry(3, 30)),
            KeyColumnValueStore.NO_DELETIONS, txh);

        // Sanity check: 3 columns present
        SliceQuery all = new SliceQuery(BufferUtil.zeroBuffer(4), BufferUtil.oneBuffer(4));
        assertEquals(3, store.getSlice(new KeySliceQuery(key, all), txh).size(),
            "Expected 3 columns before whole-row deletion");

        // Whole-row deletion with one new addition
        KCVMutation m = new KCVMutation(Arrays.asList(entry(5, 50)), KeyColumnValueStore.NO_DELETIONS);
        m.setWholeRowDeletion(true);
        mgr.mutateMany(Collections.singletonMap("edgestore", Collections.singletonMap(key, m)), txh);

        // Verify only the new column (5) is present
        SliceQuery allRange = new SliceQuery(BufferUtil.zeroBuffer(4), BufferUtil.oneBuffer(4));
        assertEquals(1, store.getSlice(new KeySliceQuery(key, allRange), txh).size(),
            "Expected 1 column after whole-row deletion with addition");
    }
}
