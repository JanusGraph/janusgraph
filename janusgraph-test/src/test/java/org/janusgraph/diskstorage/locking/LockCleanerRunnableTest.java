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

package org.janusgraph.diskstorage.locking;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import org.janusgraph.diskstorage.locking.consistentkey.StandardLockCleanerRunnable;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_END;
import static org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LockCleanerRunnableTest {

    private IMocksControl ctrl;
    private StandardLockCleanerRunnable del;
    private KeyColumnValueStore store;
    private StoreTransaction tx;

    private final ConsistentKeyLockerSerializer codec = new ConsistentKeyLockerSerializer();
    private final KeyColumn kc = new KeyColumn(
            new StaticArrayBuffer(new byte[]{(byte) 1}),
            new StaticArrayBuffer(new byte[]{(byte) 2}));
    private final StaticBuffer key = codec.toLockKey(kc.getKey(), kc.getColumn());
    private final KeySliceQuery ksq = new KeySliceQuery(key, LOCK_COL_START, LOCK_COL_END);
    private final StaticBuffer defaultLockRid = new StaticArrayBuffer(new byte[]{(byte) 32});

    @BeforeEach
    public void setupMocks() {
        IMocksControl relaxedCtrl = EasyMock.createControl();
        tx = relaxedCtrl.createMock(StoreTransaction.class);

        ctrl = EasyMock.createStrictControl();
        store = ctrl.createMock(KeyColumnValueStore.class);
    }

    @AfterEach
    public void verifyMocks() {
        ctrl.verify();
    }

    /**
     * Simplest case test of the lock cleaner.
     */
    @Test
    public void testDeleteSingleLock() throws BackendException {
        Instant now = Instant.ofEpochMilli(1L);

        Entry expiredLockCol = StaticArrayEntry.of(codec.toLockCol(now,
                defaultLockRid, TimestampProviders.MILLI), BufferUtil.getIntBuffer(0));
        EntryList expiredSingleton = StaticArrayEntryList.of(expiredLockCol);

        now = now.plusMillis(1);
        del = new StandardLockCleanerRunnable(store, kc, tx, codec, now, TimestampProviders.MILLI);

        expect(store.getSlice(eq(ksq), eq(tx)))
                .andReturn(expiredSingleton);

        store.mutate(
                eq(key),
                eq(ImmutableList.of()),
                eq(ImmutableList.of(expiredLockCol.getColumn())),
                anyObject(StoreTransaction.class));

        ctrl.replay();
        del.run();
    }

    /**
     * Test the cleaner against a set of locks where some locks have timestamps
     * before the cutoff and others have timestamps after the cutoff. One lock
     * has a timestamp equal to the cutoff.
     */
    @Test
    public void testDeletionWithExpiredAndValidLocks() throws BackendException {

        final int lockCount = 10;
        final int expiredCount = 3;
        assertTrue(expiredCount + 2 <= lockCount);
        final long timeIncrement = 1L;
        final Instant timeStart = Instant.EPOCH;
        final Instant timeCutoff = timeStart.plusMillis(expiredCount * timeIncrement);

        ImmutableList.Builder<Entry> locksBuilder = ImmutableList.builder();
        ImmutableList.Builder<Entry> deletionBuilder  = ImmutableList.builder();

        for (int i = 0; i < lockCount; i++) {
            final Instant ts = timeStart.plusMillis(timeIncrement * i);
            Entry lock = StaticArrayEntry.of(
                    codec.toLockCol(ts, defaultLockRid, TimestampProviders.MILLI),
                    BufferUtil.getIntBuffer(0));

            if (ts.isBefore(timeCutoff)) {
                deletionBuilder.add(lock);
            }

            locksBuilder.add(lock);
        }

        EntryList locks = StaticArrayEntryList.of(locksBuilder.build());
        EntryList deletions  = StaticArrayEntryList.of(deletionBuilder.build());
        assertEquals(expiredCount, deletions.size());

        del = new StandardLockCleanerRunnable(store, kc, tx, codec, timeCutoff, TimestampProviders.MILLI);

        expect(store.getSlice(eq(ksq), eq(tx))).andReturn(locks);

        store.mutate(
                eq(key),
                eq(ImmutableList.of()),
                eq(columnsOf(deletions)),
                anyObject(StoreTransaction.class));

        ctrl.replay();
        del.run();
    }

    /**
     * Locks with timestamps equal to or numerically greater than the cleaner
     * cutoff timestamp must be preserved. Test that the cleaner reads locks by
     * slicing the store and then does <b>not</b> attempt to write.
     */
    @Test
    public void testPreservesLocksAtOrAfterCutoff() throws BackendException {
        final Instant cutoff = Instant.ofEpochMilli(10L);

        Entry currentLock = StaticArrayEntry.of(codec.toLockCol(cutoff,
                defaultLockRid, TimestampProviders.MILLI), BufferUtil.getIntBuffer(0));
        Entry futureLock = StaticArrayEntry.of(codec.toLockCol(cutoff.plusMillis(1),
                defaultLockRid, TimestampProviders.MILLI), BufferUtil.getIntBuffer(0));
        EntryList locks = StaticArrayEntryList.of(currentLock, futureLock);

        // Don't increment cutoff: lockCol is exactly at the cutoff timestamp
        del = new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff, TimestampProviders.MILLI);

        expect(store.getSlice(eq(ksq), eq(tx))).andReturn(locks);

        ctrl.replay();
        del.run();
    }

    /**
     * Return a new list of {@link Entry#getColumn()} for each element in the
     * argument list.
     */
    private static List<StaticBuffer> columnsOf(List<Entry> l) {
        return Lists.transform(l, Entry::getColumn);
    }
}
