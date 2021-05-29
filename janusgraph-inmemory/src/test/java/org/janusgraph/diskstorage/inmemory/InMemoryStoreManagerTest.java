// Copyright 2019 JanusGraph Authors
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import static org.janusgraph.diskstorage.inmemory.BufferPageTest.makeEntry;
import static org.janusgraph.diskstorage.inmemory.BufferPageTest.makeStaticBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InMemoryStoreManagerTest
{
    @Test
    public void testStoreCycle() throws Exception
    {
        InMemoryStoreManager imsm = new InMemoryStoreManager();

        KeyColumnValueStore kcvs1 = imsm.openDatabase("testStore1");
        KeyColumnValueStore kcvs2 = imsm.openDatabase("testStore2");

        assertEquals("testStore1", kcvs1.getName());
        assertEquals("testStore2", kcvs2.getName());

        StoreTransaction txh = imsm.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO, imsm.getFeatures().getKeyConsistentTxConfig()));

        Map<String, Map<StaticBuffer, KCVMutation>> allMut= new HashMap<>();
        Map<StaticBuffer, KCVMutation> store1Mut = new HashMap<>();
        Map<StaticBuffer, KCVMutation> store2Mut = new HashMap<>();
        allMut.put("testStore1", store1Mut);
        allMut.put("testStore2", store2Mut);

        //first, add some columns
        List<Entry> additions1 = Arrays.asList(
                makeEntry("02col2", "val2"),
                makeEntry("01col1", "val1"),
                makeEntry("03col3", "val3"),
                makeEntry(InMemoryColumnValueStoreTest.COL_END, "valEnd"),
                makeEntry(InMemoryColumnValueStoreTest.COL_START, "valStart"),
                makeEntry("04emptycol", "")
        );

        KCVMutation kcvMut1 = new KCVMutation(additions1, Collections.EMPTY_LIST);
        store1Mut.put(makeStaticBuffer("row1"), kcvMut1);

        List<Entry> additions2 = Arrays.asList(
                makeEntry("01col1", "val1"),
                makeEntry("03col3", "val3")
        );
        KCVMutation kcvMut2 = new KCVMutation(additions2, Collections.EMPTY_LIST);
        store2Mut.put(makeStaticBuffer("row1"), kcvMut2);

        imsm.mutateMany(allMut, txh);

        EntryList result1 = kcvs1.getSlice(new KeySliceQuery(makeStaticBuffer("row1"),
                    makeStaticBuffer(InMemoryColumnValueStoreTest.COL_START),
                    makeStaticBuffer(InMemoryColumnValueStoreTest.VERY_END)
                ),
                txh);

        Map<StaticBuffer, EntryList> result2 = kcvs2.getSlice(
                Arrays.asList(makeStaticBuffer("row1"), makeStaticBuffer("row2")),
                new KeySliceQuery(makeStaticBuffer("row1"),
                makeStaticBuffer(InMemoryColumnValueStoreTest.COL_START),
                makeStaticBuffer(InMemoryColumnValueStoreTest.VERY_END)
                ),
                txh);

        assertEquals(2, result2.size());

        assertEquals(additions1.size(), result1.size());
        assertEquals(additions2.size(), result2.get(makeStaticBuffer("row1")).size());
        assertEquals(0, result2.get(makeStaticBuffer("row2")).size());

        for (boolean rollbackIfFailed : new boolean[] {true, false})
        {
            File testSnapshotDir = new File(SystemUtils.JAVA_IO_TMPDIR, Long.toString(System.currentTimeMillis()));
            testSnapshotDir.deleteOnExit();

            try
            {
                imsm.makeSnapshot(testSnapshotDir, ForkJoinPool.commonPool());

                imsm.clearStorage(); //to make the fact that the previous contents were cleared visible

                imsm.restoreFromSnapshot(testSnapshotDir, rollbackIfFailed, ForkJoinPool.commonPool());

                kcvs1 = imsm.openDatabase("testStore1");
                kcvs2 = imsm.openDatabase("testStore2");

                EntryList result1AfterRestore = kcvs1.getSlice(new KeySliceQuery(makeStaticBuffer("row1"),
                                makeStaticBuffer(InMemoryColumnValueStoreTest.COL_START),
                                makeStaticBuffer(InMemoryColumnValueStoreTest.VERY_END)
                        ),
                        txh);

                Map<StaticBuffer, EntryList> result2AfterRestore = kcvs2.getSlice(
                    Arrays.asList(makeStaticBuffer("row1"), makeStaticBuffer("row2")),
                        new KeySliceQuery(makeStaticBuffer("row1"),
                                makeStaticBuffer(InMemoryColumnValueStoreTest.COL_START),
                                makeStaticBuffer(InMemoryColumnValueStoreTest.VERY_END)
                        ),
                        txh);

                assertEquals(result1, result1AfterRestore);
                assertEquals(result2, result2AfterRestore);
            } finally
            {
                FileUtils.cleanDirectory(testSnapshotDir);
                testSnapshotDir.delete();
            }
        }

        imsm.close();
    }
}
