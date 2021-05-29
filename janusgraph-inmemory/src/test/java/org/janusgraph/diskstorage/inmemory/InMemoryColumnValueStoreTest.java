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

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.janusgraph.diskstorage.inmemory.BufferPageTest.makeEntry;
import static org.janusgraph.diskstorage.inmemory.BufferPageTest.makeStaticBuffer;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a unit test for custom override of JanusGraph ColumnValueStore. Required to verify the added logic and
 * the correctness of its integration with standard logic
 */
public class InMemoryColumnValueStoreTest
{
    public static final String VERY_START = Character.valueOf((char)0).toString();
    public static final String COL_START = "00colStart";
    public static final String COL_END = "99colEnd";
    public static final String VERY_END = "zz";

    @Test
    public void testRoundtrip()
    {
        InMemoryColumnValueStore[] cvStores = new InMemoryColumnValueStore[] {
                new InMemoryColumnValueStore(),
                //same as above but page size = 3 to catch multi-page cases which otherwise
                // go untested with default page size of 500 due to small sample data set
                new TestPagedBufferColumnValueStore(3)
        };

        //first, add some columns
        List<Entry> additions1 = Arrays.asList(
                makeEntry("02col2", "val2"),
                makeEntry("01col1", "val1"),
                makeEntry("03col3", "val3"),
                makeEntry(COL_END, "valEnd"),
                makeEntry(COL_START, "valStart"),
                makeEntry("04emptycol", "")
        );

        List<StaticBuffer> deletions1 = Collections.emptyList();

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        for(InMemoryColumnValueStore cvs : cvStores)
        {
            cvs.mutate(additions1, deletions1, txh);
        }

        EntryList[] results = new EntryList[cvStores.length];

        //check the added/retrieved columns against originals
        for(int i=0; i<cvStores.length; i++)
        {
            results[i] = cvStores[i].getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                    makeStaticBuffer(COL_START), makeStaticBuffer(VERY_END)),
                    txh //if we pass COL_END, it doesn't get included
            );

            assertEquals(additions1.size(), results[i].size());
        }

        //we'll get the results back sorted, so we sort the initial entries as well, for ease of comparison
        additions1.sort(Comparator.naturalOrder());

        for(int j=0; j<cvStores.length; j++)
        {
            for (int i=0; i< results[j].size(); i++)
            {
                assertEquals(additions1.get(i), results[j].get(i));
                assertEquals(additions1.get(i), results[j].get(i));
                assertEquals(additions1.get(i).getColumn(), results[j].get(i).getColumn());
                assertEquals(additions1.get(i).getValue(), results[j].get(i).getValue());
            }
        }

        //now delete and update some columns
        List<Entry> additions2 = Arrays.asList(
                makeEntry("02col2", "val2_updated"),
                makeEntry("03col3", "val3_updated")
        );

        List<StaticBuffer> deletions2 = Arrays.asList(
            makeStaticBuffer("01col1"),
            makeStaticBuffer("02col2")
        );

        for(InMemoryColumnValueStore cvs : cvStores)
        {
            cvs.mutate(additions2, deletions2, txh);
        }

        for(int i=0; i<cvStores.length; i++)
        {
            results[i] = cvStores[i].getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                    makeStaticBuffer(COL_START), makeStaticBuffer(VERY_END)),
                    txh //if we pass COL_END, it doesn't get included
            );

            //col2 was in both deletes AND updates, JanusGraph logic is to honour update over delete, so we expect only col1 to be deleted
            assertEquals(additions1.size()-1, results[i].size());
        }

        for(int i=0; i<cvStores.length; i++)
        {
            final Map<StaticBuffer, StaticBuffer> seenValues = new HashMap<>(additions1.size()-1);

            StaticArrayBuffer expectDeleted = makeStaticBuffer("01col1");

            results[i].forEach(entry -> seenValues.put(entry.getColumn(), entry.getValue()));

            assertFalse(seenValues.containsKey(expectDeleted));

            additions2.forEach(entry ->
            {
                assertTrue(seenValues.containsKey(entry.getColumn()));
                assertEquals(seenValues.get(entry.getColumn()), entry.getValue());
            });
        }


        for(int i=0; i<cvStores.length; i++)
        {
            results[i] = cvStores[i].getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                    additions1.get(1).getColumn(),
                    additions1.get(3).getColumn()),
                    txh //if we pass COL_END, it doesn't get included
            );

            assertEquals(1, results[i].size());
        }

    }

    @Test
    public void testVolumeSlidingWindows() throws TemporaryLockingException
    {
        int pageSize = InMemoryColumnValueStore.DEF_PAGE_SIZE;
        int maxNumEntries = 7*pageSize + pageSize/2;

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        InMemoryColumnValueStore cvs = new InMemoryColumnValueStore();

        for(int numEntries=1;numEntries<maxNumEntries;numEntries+=pageSize + pageSize/3)
        {
            List<Entry> additions = generateEntries(0, numEntries,"orig");

            cvs.mutate(additions, Collections.emptyList(), txh);

            EntryList result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                    makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
                    txh
                 );

            assertEquals(additions.size(), result.size());

            int stepSize = numEntries < 100 ? 1 : 21;

            for (int windowSize=0; windowSize<numEntries; windowSize+=stepSize)
            {
                for (int windowStart=0; windowStart<numEntries; windowStart+=stepSize)
                {
                    int windowEnd = Math.min(windowStart+windowSize, numEntries-1);
                    result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                            additions.get(windowStart).getColumn(),
                            additions.get(windowEnd).getColumn()
                    ),
                           txh);
                    assertEquals(windowEnd - windowStart, result.size());
                }
            }
        }
    }

    @Test
    public void testMultipageSlicing() throws TemporaryLockingException
    {
        int numEntries = 502;
        int windowStart = 494;
        int windowEnd = 501;

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        InMemoryColumnValueStore cvs = new InMemoryColumnValueStore();
        //ColumnValueStore cvs = new DeflatedEntryColumnValueStore(false);
        List<Entry> additions = generateEntries(0, numEntries,"orig");

        cvs.mutate(additions, Collections.emptyList(), txh);

        EntryList result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START),
                makeStaticBuffer(VERY_END)), txh //if we pass COL_END, it doesn't get included
                );

        assertEquals(additions.size(), result.size());

        //this getSlice spans two pages and doesn't retrieve either page in full
        result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                additions.get(windowStart).getColumn(),
                additions.get(windowEnd).getColumn()
                ), txh);

        assertEquals(windowEnd - windowStart, result.size());
    }

    @Test
    public void testMultipageDelete() throws TemporaryLockingException
    {
        int numEntries = 1001;

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        InMemoryColumnValueStore cvs = new InMemoryColumnValueStore();
        //ColumnValueStore cvs = new DeflatedEntryColumnValueStore(false);
        List<Entry> additions = generateEntries(0, numEntries,"orig");

        cvs.mutate(additions, Collections.emptyList(), txh);

        EntryList result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions.size(), result.size());

        int windowStart = 494;
        int windowEnd = 501;

        List<StaticBuffer> deletions = new ArrayList<>(windowEnd-windowStart);

        deletions.addAll(additions.subList(windowStart,windowEnd).stream().map(Entry::getColumn).collect(Collectors.toList()));

        cvs.mutate(Collections.emptyList(), deletions, txh);

        result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions.size()-deletions.size(), result.size());

    }

    @Test
    public void testMultipageUpdateDelete() throws TemporaryLockingException
    {
        int numEntries = 2511;

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        InMemoryColumnValueStore cvs = new InMemoryColumnValueStore();
        //ColumnValueStore cvs = new DeflatedEntryColumnValueStore(false);
        List<Entry> additions = generateEntries(0, numEntries,"orig");

        cvs.mutate(additions, Collections.emptyList(), txh);

        EntryList result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions.size(), result.size());

        int windowStart = 494;
        int windowEnd = 2002;

        //update
        List<Entry> updates = generateEntries(windowStart, windowEnd, "updated");

        cvs.mutate(updates, Collections.emptyList(), txh);

        result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions.size(), result.size());

        for(int i=0; i<result.size(); i++)
        {
            if (windowStart < i && i < windowEnd)
            {
                assertEquals(updates.get(i-windowStart), result.get(i));
            }
            else
            {
                assertEquals(additions.get(i), result.get(i));
            }
        }

        //delete
        List<StaticBuffer> deletions = new ArrayList<>(windowEnd-windowStart);
        deletions.addAll(additions.subList(windowStart,windowEnd).stream().map(Entry::getColumn).collect(Collectors.toList()));
        cvs.mutate(Collections.emptyList(), deletions, txh);

        result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions.size()-deletions.size(), result.size());
    }

    @Test
    public void testAddInterleaved() throws TemporaryLockingException
    {
        int pageSize = InMemoryColumnValueStore.DEF_PAGE_SIZE;

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        InMemoryColumnValueStore cvs = new InMemoryColumnValueStore();

        List<Entry> additions1 = generateEntries(0, pageSize*2 + pageSize/2,"orig");
        cvs.mutate(additions1, Collections.emptyList(), txh);

        List<Entry> additions2 = generateEntries(pageSize*10, pageSize*10 + pageSize*2 + pageSize/2,"orig");
        cvs.mutate(additions2, Collections.emptyList(), txh);

        List<Entry> additions3 = generateEntries(pageSize*5, pageSize*5 + pageSize*2 + pageSize/2,"orig");
        cvs.mutate(additions3, Collections.emptyList(), txh);

        EntryList result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions1.size()+additions2.size()+additions3.size(), result.size());

        for(int i=0; i< result.size() -1; i++)
        {
            assertTrue(result.get(i).compareTo(result.get(i+1)) < 0);
        }
    }

    @Test
    public void testPagingAndFragmentation() throws TemporaryLockingException
    {
        int pageSize = InMemoryColumnValueStore.DEF_PAGE_SIZE;

        StoreTransaction txh = mock(StoreTransaction.class);
        BaseTransactionConfig mockConfig = mock(BaseTransactionConfig.class);
        when(txh.getConfiguration()).thenReturn(mockConfig);
        when(mockConfig.getCustomOption(eq(STORAGE_TRANSACTIONAL))).thenReturn(true);

        InMemoryColumnValueStore cvs = new InMemoryColumnValueStore();

        List<Entry> additions = generateEntries(0, pageSize*5 + pageSize/2,"orig");
        cvs.mutate(additions, Collections.emptyList(), txh);
        //We inserted more than pagesize in one go, so the store should switch to multipage buffer - but, as currently implemented,
        // this doesn't get immediately broken up into multiple pages, instead it will hold a single "oversized" page.
        //The break-up will only happen if there is a consequent update
        assertEquals(1, cvs.numPages(txh));

        //emulate update so that the single "oversized" page will be broken up into multiple pages of correct size
        cvs.mutate(additions.subList(1, 3), Collections.emptyList(), txh);
        assertEquals(6, cvs.numPages(txh));

        int numDeleted = 0;

        int windowStart = pageSize - pageSize/3;
        int windowEnd = pageSize + pageSize/3;
        //this should remove parts of page0 and page1
        List<StaticBuffer> deletions = new ArrayList<>(windowEnd-windowStart);
        deletions.addAll(additions.subList(windowStart,windowEnd).stream().map(Entry::getColumn).collect(Collectors.toList()));
        cvs.mutate(Collections.emptyList(), deletions, txh);

        numDeleted += windowEnd - windowStart;
        EntryList result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);

        assertEquals(additions.size() - numDeleted, result.size());

        SharedEntryBufferFragmentationReport report = cvs.createFragmentationReport(txh);
        assertEquals(6, report.getPageCount());
        assertEquals(3, report.getFragmentedPageCount());
        //since only 1/3 of each page is removed, the remains won't fit into one page anyway, so not deemed compressable
        assertEquals(0, report.getCompressableChunksCount());
        assertEquals(0, report.getCompressablePageCount());

        windowStart = pageSize * 4 - pageSize/3;
        windowEnd = pageSize * 4 + pageSize/3;
        //this should remove  parts of page3 and page 4
        deletions.clear();
        deletions.addAll(additions.subList(windowStart,windowEnd).stream().map(Entry::getColumn).collect(Collectors.toList()));
        cvs.mutate(Collections.emptyList(), deletions, txh);

        numDeleted += windowEnd - windowStart;
        result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);
        assertEquals(additions.size() - numDeleted, result.size());

        report = cvs.createFragmentationReport(txh);
        assertEquals(6, report.getPageCount());
        assertEquals(5, report.getFragmentedPageCount());
        //we now have pages 3 & 4 which are 2/3 full, PLUS page 5 which is half full => 3 pages compressable into 2
        assertEquals(1, report.getCompressableChunksCount());
        assertEquals(3, report.getCompressablePageCount());
        assertEquals(1, report.getAchievablePageReduction());

        cvs.quickDefragment(txh);

        EntryList result2 = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);
        assertEquals(additions.size() - numDeleted, result2.size());
        for(int i=0; i< result2.size(); i++)
        {
            assertEquals(result.get(i), result2.get(i));
        }

        //after quick defrag, we should have 5 pages in total, page 0 & 1 still fragmented, page 4 also not full
        report = cvs.createFragmentationReport(txh);
        assertEquals(5, report.getPageCount());
        assertEquals(3, report.getFragmentedPageCount());
        assertEquals(0, report.getCompressableChunksCount());
        assertEquals(0, report.getCompressablePageCount());
        assertEquals(0, report.getAchievablePageReduction());

        windowStart = pageSize - pageSize/2;
        windowEnd = pageSize + pageSize/2+1;
        //this should remove half of page0 and page1 each
        deletions.clear();
        deletions.addAll(additions.subList(windowStart,windowEnd).stream().map(Entry::getColumn).collect(Collectors.toList()));
        cvs.mutate(Collections.emptyList(), deletions, txh);

        numDeleted += (pageSize/2 - pageSize/3) * 2 + 1;
        result = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);
        assertEquals(additions.size() - numDeleted, result.size());

        //now two first pages should become collapsible into one
        report = cvs.createFragmentationReport(txh);
        assertEquals(5, report.getPageCount());
        assertEquals(3, report.getFragmentedPageCount());
        assertEquals(1, report.getCompressableChunksCount());
        assertEquals(2, report.getCompressablePageCount());
        assertEquals(1, report.getAchievablePageReduction());

        cvs.quickDefragment(txh);

        result2 = cvs.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                makeStaticBuffer(VERY_START), makeStaticBuffer(VERY_END)), //if we pass COL_END, it doesn't get included
            txh);
        assertEquals(additions.size() - numDeleted, result2.size());
        for(int i=0; i< result2.size(); i++)
        {
            assertEquals(result.get(i), result2.get(i));
        }

        //two first pages collapsed into one which is one entry short of full
        report = cvs.createFragmentationReport(txh);
        assertEquals(4, report.getPageCount());
        assertEquals(2, report.getFragmentedPageCount());
        assertEquals(0, report.getCompressableChunksCount());
        assertEquals(0, report.getCompressablePageCount());
        assertEquals(0, report.getAchievablePageReduction());
    }

    public static List<Entry> generateEntries(int start, int end, String suffix)
    {
        List<Entry> entries = new ArrayList<>(end-start);
        for(int i=start; i<end; i++)
        {
            entries.add(makeEntry(String.format("%012dcol",i),
                    String.format("val%d_%s",i,suffix)));
        }

        return entries;
    }
}
