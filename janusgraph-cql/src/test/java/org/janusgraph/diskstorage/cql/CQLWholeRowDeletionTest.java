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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.AbstractKCVSTest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class CQLWholeRowDeletionTest extends AbstractKCVSTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    private KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CachingCQLStoreManager(cqlContainer.getConfiguration(getClass().getSimpleName()));
    }

    private static Entry e(int col, int val) {
        return StaticArrayEntry.of(BufferUtil.getIntBuffer(col), BufferUtil.getIntBuffer(val));
    }

    @Test
    public void featureAdvertised() throws Exception {
        KeyColumnValueStoreManager mgr = openStorageManager();
        try {
            assertTrue(mgr.getFeatures().hasOptimizedWholeRowDeletion(),
                "CQL backend must advertise hasOptimizedWholeRowDeletion=true");
        } finally {
            mgr.close();
        }
    }

    @Test
    public void wholeRowDeletionEmptiesPartition() throws Exception {
        KeyColumnValueStoreManager mgr = openStorageManager();
        try {
            KeyColumnValueStore store = mgr.openDatabase("wrdtest");

            // Write 3 columns to a key
            StoreTransaction tx = mgr.beginTransaction(getTxConfig());
            StaticBuffer key = BufferUtil.getIntBuffer(42);
            store.mutate(key, Arrays.asList(e(1, 1), e(2, 2), e(3, 3)), KeyColumnValueStore.NO_DELETIONS, tx);
            tx.commit();

            // Verify 3 columns are present
            tx = mgr.beginTransaction(getTxConfig());
            SliceQuery all = new SliceQuery(BufferUtil.getIntBuffer(0), BufferUtil.getIntBuffer(Integer.MAX_VALUE));
            assertEquals(3, store.getSlice(new KeySliceQuery(key, all), tx).size(),
                "Expected 3 columns before whole-row deletion");
            tx.rollback();

            // Issue whole-row deletion
            tx = mgr.beginTransaction(getTxConfig());
            KCVMutation m = new KCVMutation(KeyColumnValueStore.NO_ADDITIONS, KeyColumnValueStore.NO_DELETIONS);
            m.setWholeRowDeletion(true);
            mgr.mutateMany(Collections.singletonMap("wrdtest", Collections.singletonMap(key, m)), tx);
            tx.commit();

            // Verify partition is now empty
            tx = mgr.beginTransaction(getTxConfig());
            assertEquals(0, store.getSlice(new KeySliceQuery(key, all), tx).size(),
                "Expected 0 columns after whole-row deletion");
            tx.rollback();
        } finally {
            mgr.close();
        }
    }
}
