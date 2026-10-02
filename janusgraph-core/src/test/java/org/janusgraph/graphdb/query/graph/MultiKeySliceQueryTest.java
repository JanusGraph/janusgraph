// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.query.graph;

import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A composite index query reads its keys together where that reads no key which reading them one after the other would
 * not, and returns exactly what reading them one after the other returns. Most tests run the query both ways, against
 * two stores with the same contents, and compare the results and the keys read.
 */
public class MultiKeySliceQueryTest {

    private static final boolean UNIQUE = true;
    private static final boolean NOT_UNIQUE = false;

    //An index store behind a mocked transaction: each key holds the entries given, and a read returns the first of them
    //up to the limit of its slice, as a storage backend does. It records the reads
    private static final class Store {
        private final Map<StaticBuffer, List<Entry>> rows = new HashMap<>();
        private final List<StaticBuffer> singleKeyReads = new ArrayList<>();
        private final List<List<StaticBuffer>> multiKeyReads = new ArrayList<>();
        private final List<SliceQuery> multiKeySlices = new ArrayList<>();
        private boolean leavesOutEmptyRows;

        //Some stores answer a multi-key read without the keys which hold nothing
        Store leavingOutEmptyRows() {
            leavesOutEmptyRows = true;
            return this;
        }

        Store holding(int key, int entries) {
            final List<Entry> row = new ArrayList<>(entries);
            for (int i = 0; i < entries; i++) {
                row.add(StaticArrayEntry.of(StaticArrayBuffer.of(new byte[]{(byte) i}),
                    StaticArrayBuffer.of(new byte[]{(byte) (key >> 8), (byte) key, (byte) i})));
            }
            rows.put(key(key), row);
            return this;
        }

        private EntryList read(StaticBuffer key, SliceQuery slice) {
            List<Entry> row = rows.getOrDefault(key, Collections.emptyList());
            if (slice.hasLimit()) {
                row = row.subList(0, Math.min(slice.getLimit(), row.size()));
            }
            return EntryArrayList.of(row);
        }

        BackendTransaction transaction(boolean multiKeyQueries) {
            final BackendTransaction tx = mock(BackendTransaction.class);
            when(tx.hasMultiKeyIndexQueries()).thenReturn(multiKeyQueries);
            when(tx.indexQuery(any(KeySliceQuery.class))).thenAnswer(invocation -> {
                final KeySliceQuery query = invocation.getArgument(0);
                singleKeyReads.add(query.getKey());
                return read(query.getKey(), query);
            });
            when(tx.indexStoreMultiQuery(anyList(), any(SliceQuery.class))).thenAnswer(invocation -> {
                final List<StaticBuffer> keys = invocation.getArgument(0);
                final SliceQuery slice = invocation.getArgument(1);
                multiKeyReads.add(new ArrayList<>(keys));
                multiKeySlices.add(slice);
                final Map<StaticBuffer, EntryList> result = new HashMap<>();
                for (StaticBuffer key : keys) {
                    final EntryList entries = read(key, slice);
                    if (!entries.isEmpty() || !leavesOutEmptyRows) {
                        result.put(key, entries);
                    }
                }
                return result;
            });
            return tx;
        }

        int keysRead() {
            return singleKeyReads.size() + multiKeyReads.stream().mapToInt(List::size).sum();
        }
    }

    private static StaticBuffer key(int key) {
        return StaticArrayBuffer.of(new byte[]{(byte) (key >> 8), (byte) key});
    }

    private static MultiKeySliceQuery query(boolean unique, int... keys) {
        final List<KeySliceQuery> queries = new ArrayList<>(keys.length);
        for (int key : keys) {
            queries.add(new KeySliceQuery(key(key), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)));
        }
        return new MultiKeySliceQuery(queries, unique);
    }

    private static List<List<String>> contents(List<EntryList> result) {
        return result.stream()
            .map(entries -> entries.stream().map(entry -> entry.getColumn() + "=" + entry.getValue()).collect(Collectors.toList()))
            .collect(Collectors.toList());
    }

    //Runs the query reading keys together where it may, checks it against reading them one after the other, and
    //returns the store it read from
    private static Store runBothWays(MultiKeySliceQuery query, Store together, Store oneByOne) {
        assertSameResult(query, together, oneByOne);
        assertTrue(together.keysRead() <= oneByOne.keysRead(), "read keys which reading one after the other doesn't");
        return together;
    }

    private static void assertSameResult(MultiKeySliceQuery query, Store together, Store oneByOne) {
        final List<EntryList> expected = query.execute(oneByOne.transaction(false), true);
        assertTrue(oneByOne.multiKeyReads.isEmpty());
        final List<EntryList> actual = query.execute(together.transaction(true), true);
        assertEquals(contents(expected), contents(actual));
    }

    private static Store store() {
        return new Store().holding(1, 2).holding(2, 0).holding(3, 1).holding(4, 3).holding(5, 1);
    }

    @Test
    public void shouldReadEveryKeyInOneCallWithoutALimit() {
        final Store store = runBothWays(query(NOT_UNIQUE, 1, 2, 3, 4, 5), store(), store());

        assertEquals(Collections.singletonList(Arrays.asList(key(1), key(2), key(3), key(4), key(5))), store.multiKeyReads);
        assertFalse(store.multiKeySlices.get(0).hasLimit());
        assertTrue(store.singleKeyReads.isEmpty());
    }

    @Test
    public void shouldReadARecurringKeyOnce() {
        final Store store = runBothWays(query(NOT_UNIQUE, 1, 4, 1), store(), store());

        assertEquals(Collections.singletonList(Arrays.asList(key(1), key(4))), store.multiKeyReads);
    }

    @Test
    public void shouldReadARecurringKeyOnceWithALimit() {
        final Store store = runBothWays(query(UNIQUE, 3, 3, 5, 3).updateLimit(3),
            new Store().holding(3, 1).holding(5, 1), new Store().holding(3, 1).holding(5, 1));

        assertEquals(Collections.singletonList(Arrays.asList(key(3), key(5))), store.multiKeyReads);
    }

    private static int[] keys(int count) {
        final int[] keys = new int[count];
        for (int i = 0; i < count; i++) {
            keys[i] = i;
        }
        return keys;
    }

    private static List<Integer> callSizes(Store store) {
        return store.multiKeyReads.stream().map(List::size).collect(Collectors.toList());
    }

    @Test
    public void shouldReadAtMostAThousandKeysPerCall() {
        final Store store = runBothWays(query(NOT_UNIQUE, keys(2_500)), store(), store());

        assertEquals(1_000, MultiKeySliceQuery.MAX_KEYS_PER_READ);
        assertEquals(Arrays.asList(1_000, 1_000, 500), callSizes(store));
    }

    @Test
    public void shouldReadAtMostAThousandKeysPerCallWithALimit() {
        final Store together = new Store();
        final Store oneByOne = new Store();
        for (int key = 0; key < 2_500; key++) {
            together.holding(key, 1);
            oneByOne.holding(key, 1);
        }

        final Store store = runBothWays(query(UNIQUE, keys(2_500)).updateLimit(1_500), together, oneByOne);

        assertEquals(Arrays.asList(1_000, 500), callSizes(store));
        assertEquals(1_500, store.keysRead());
    }

    @Test
    public void shouldTreatAKeyLeftOutOfAMultiKeyAnswerAsEmpty() {
        final Store store = runBothWays(query(NOT_UNIQUE, 1, 2, 3), store().leavingOutEmptyRows(), store());

        assertEquals(1, store.multiKeyReads.size());
    }

    //execute(tx) keeps the behaviour it always had, whatever the backend supports
    @Test
    public void shouldReadOneKeyAfterTheOtherThroughTheOneArgumentExecute() {
        final Store store = store();
        final List<EntryList> result = query(NOT_UNIQUE, 1, 2, 3, 4, 5).execute(store.transaction(true));

        assertEquals(5, result.size());
        assertTrue(store.multiKeyReads.isEmpty());
        assertEquals(5, store.singleKeyReads.size());
    }

    //query.batch.enabled = false turns reading keys together off
    @Test
    public void shouldReadOneKeyAfterTheOtherWhenTheTransactionDoesNotBatch() {
        final Store store = store();
        final List<EntryList> together = query(NOT_UNIQUE, 1, 2, 3, 4, 5).execute(store.transaction(true), false);

        assertEquals(5, together.size());
        assertTrue(store.multiKeyReads.isEmpty());
        assertEquals(5, store.singleKeyReads.size());
    }

    //Each key of a unique index holds at most one entry, so a call for as many keys as the limit can still take reads
    //no key which reading them one after the other would not
    @Test
    public void shouldReadAsManyKeysAsTheLimitCanStillTakeForAUniqueIndex() {
        final Store together = new Store().holding(1, 1).holding(4, 1).holding(5, 1).holding(6, 1).holding(8, 1);
        final Store oneByOne = new Store().holding(1, 1).holding(4, 1).holding(5, 1).holding(6, 1).holding(8, 1);

        final Store store = runBothWays(query(UNIQUE, 1, 2, 3, 4, 5, 6, 7, 8).updateLimit(4), together, oneByOne);

        assertEquals(Arrays.asList(Arrays.asList(key(1), key(2), key(3), key(4)), Arrays.asList(key(5), key(6))),
            store.multiKeyReads);
        assertEquals(4, store.multiKeySlices.get(0).getLimit());
        assertEquals(2, store.multiKeySlices.get(1).getLimit());
        assertEquals(oneByOne.keysRead(), store.keysRead());
    }

    @Test
    public void shouldReadEveryKeyOfAUniqueIndexInOneCallWhenTheLimitExceedsThem() {
        final Store store = runBothWays(query(UNIQUE, 3, 5, 9).updateLimit(100_000),
            new Store().holding(3, 1).holding(5, 1), new Store().holding(3, 1).holding(5, 1));

        assertEquals(Collections.singletonList(Arrays.asList(key(3), key(5), key(9))), store.multiKeyReads);
    }

    //Key 4 holds three entries although the query is told each key holds at most one, which a unique index rules out:
    //its one column doesn't name the element. Even then the query returns what reading one after the other returns,
    //only the keys it reads are no longer bound to theirs
    @Test
    public void shouldReturnNoMoreOfAKeysEntriesThanTheRestOfTheLimit() {
        assertSameResult(query(UNIQUE, 3, 4, 5).updateLimit(2), store(), store());
        assertSameResult(query(UNIQUE, 3, 4, 5).updateLimit(3), store(), store());
    }

    @Test
    public void shouldReadOneKeyAfterTheOtherWithALimitForAnIndexWhichIsNotUnique() {
        final Store store = runBothWays(query(NOT_UNIQUE, 1, 2, 3, 4, 5).updateLimit(3), store(), store());

        assertTrue(store.multiKeyReads.isEmpty());
        assertEquals(Arrays.asList(key(1), key(2), key(3)), store.singleKeyReads);
    }

    @Test
    public void shouldReadOneKeyAfterTheOtherWithoutMultiKeyQueries() {
        final Store store = store();
        final List<EntryList> result = query(NOT_UNIQUE, 1, 2, 3, 4, 5).execute(store.transaction(false), true);

        assertEquals(5, result.size());
        assertTrue(store.multiKeyReads.isEmpty());
        assertEquals(5, store.singleKeyReads.size());
    }

    @Test
    public void shouldReadASingleKeyOnItsOwn() {
        final Store store = runBothWays(query(NOT_UNIQUE, 4), store(), store());

        assertTrue(store.multiKeyReads.isEmpty());
        assertEquals(Collections.singletonList(key(4)), store.singleKeyReads);
    }

    @Test
    public void shouldReadOneKeyAfterTheOtherForALimitOfZero() {
        final Store store = runBothWays(query(UNIQUE, 1, 3, 5).updateLimit(0), store(), store());

        assertTrue(store.multiKeyReads.isEmpty());
    }

    //Changing the list after building the query, here to a key of another slice, changes neither the keys it reads nor
    //that it reads them together
    @Test
    public void shouldKeepItsOwnCopyOfTheKeyQueries() {
        final List<KeySliceQuery> queries = new ArrayList<>(Arrays.asList(
            new KeySliceQuery(key(1), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)),
            new KeySliceQuery(key(3), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1))));
        final MultiKeySliceQuery query = new MultiKeySliceQuery(queries);
        queries.set(1, new KeySliceQuery(key(4), BufferUtil.zeroBuffer(2), BufferUtil.oneBuffer(2)));
        queries.add(new KeySliceQuery(key(5), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)));

        final Store store = runBothWays(query, store(), store());

        assertEquals(Collections.singletonList(Arrays.asList(key(1), key(3))), store.multiKeyReads);
        assertTrue(store.singleKeyReads.isEmpty());
        assertEquals(query, new MultiKeySliceQuery(Arrays.asList(
            new KeySliceQuery(key(1), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)),
            new KeySliceQuery(key(3), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)))));
    }

    @Test
    public void shouldReadOneKeyAfterTheOtherWhenTheSlicesDiffer() {
        final List<KeySliceQuery> queries = Arrays.asList(
            new KeySliceQuery(key(1), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)),
            new KeySliceQuery(key(3), BufferUtil.zeroBuffer(2), BufferUtil.oneBuffer(2)));

        final Store store = runBothWays(new MultiKeySliceQuery(queries), store(), store());

        assertTrue(store.multiKeyReads.isEmpty());
        assertEquals(2, store.singleKeyReads.size());
    }
}
