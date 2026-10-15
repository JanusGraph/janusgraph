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

package org.janusgraph.diskstorage.berkeleyje;

import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore.NO_DELETIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lock tests on BerkeleyJE, whose store managers here all open one directory, as the graph instances of a JVM which
 * open one graph do: they share its environment, and the locks of its keys.
 */
public class BerkeleyLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    private static final String NO_EXPIRY = "A BerkeleyJE lock is held until its transaction commits or rolls back";

    private final StaticBuffer key = KeyValueStoreUtil.getBuffer("testkey");
    private final StaticBuffer column = KeyValueStoreUtil.getBuffer("col1");
    private final StaticBuffer value = KeyValueStoreUtil.getBuffer("val1");
    private final StaticBuffer otherValue = KeyValueStoreUtil.getBuffer("val2");

    @Override
    public KeyColumnValueStoreManager openStorageManager(int id, Configuration configuration) throws BackendException {
        return openStorageManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
    }

    private static KeyColumnValueStoreManager openStorageManager(Configuration configuration) throws BackendException {
        return new OrderedKeyValueStoreManagerAdapter(new BerkeleyJEStoreManager(configuration));
    }

    private List<Entry> entry(StaticBuffer entryValue) {
        return Collections.singletonList(StaticArrayEntry.of(column, entryValue));
    }

    /**
     * The managers share their locks, so a transaction of one fails at once to take a lock which a transaction of
     * another holds, rather than when it writes, as against another process.
     */
    @Override
    @Test
    public void testRemoteLockContention() throws BackendException {
        store[0].acquireLock(key, column, null, tx[0][0]);
        assertThrows(PermanentLockingException.class, () -> store[1].acquireLock(key, column, null, tx[1][0]));
        store[0].mutate(key, entry(value), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();
        tx[0][0] = newTransaction(manager[0]);

        store[1].acquireLock(key, column, value, tx[1][1]);
        store[1].mutate(key, entry(otherValue), NO_DELETIONS, tx[1][1]);
        tx[1][1].commit();
        tx[1][1] = newTransaction(manager[1]);
        assertEquals(otherValue, KCVSUtil.get(store[0], key, column, tx[0][0]));
    }

    @Override
    @Test
    @Disabled(NO_EXPIRY)
    public void expiredLocalLockIsIgnored() {
    }

    @Override
    @Test
    @Disabled(NO_EXPIRY)
    public void expiredRemoteLockIsIgnored() {
    }

    @Override
    @Test
    @Disabled(NO_EXPIRY)
    public void repeatLockingDoesNotExtendExpiration() {
    }

    @Override
    @Test
    @Disabled("BerkeleyJE doesn't use the expected value checking store which this test runs with a mock locker; it "
        + "closes the store manager while its databases are open, which BerkeleyJE refuses, and leaves its BerkeleyJE "
        + "transactions open")
    public void testLocksOnMultipleStores() {
    }

    @Test
    public void testCommitAndRollbackReleaseTheLocks() throws BackendException {
        store[0].acquireLock(key, column, null, tx[0][0]);
        tx[0][0].rollback();
        tx[0][0] = newTransaction(manager[0]);

        store[1].acquireLock(key, column, null, tx[1][0]);
        store[1].mutate(key, entry(value), NO_DELETIONS, tx[1][0]);
        tx[1][0].commit();
        tx[1][0] = newTransaction(manager[1]);

        store[0].acquireLock(key, column, value, tx[0][0]);
        store[0].mutate(key, entry(otherValue), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();
        tx[0][0] = newTransaction(manager[0]);
        assertEquals(otherValue, KCVSUtil.get(store[1], key, column, tx[1][0]));
    }

    /**
     * The expected value is compared with the value which the key has when the transaction first writes, which a
     * transaction which didn't lock the key changed after the lock was taken.
     */
    @Test
    public void testExpectedValueIsCheckedWhenTheTransactionFirstWrites() throws BackendException {
        store[0].acquireLock(key, column, null, tx[0][0]);
        store[1].mutate(key, entry(value), NO_DELETIONS, tx[1][0]);
        tx[1][0].commit();
        tx[1][0] = newTransaction(manager[1]);

        final PermanentLockingException failure = assertThrows(PermanentLockingException.class,
            () -> store[0].mutate(key, entry(otherValue), NO_DELETIONS, tx[0][0]));
        assertTrue(failure.getMessage().startsWith("Expected value mismatch for"), failure::toString);
        tx[0][0].rollback();
        tx[0][0] = newTransaction(manager[0]);
        assertEquals(value, KCVSUtil.get(store[0], key, column, tx[0][0]));
    }

    @Test
    public void testLockAfterTheFirstWriteFails() throws BackendException {
        store[0].mutate(key, entry(value), NO_DELETIONS, tx[0][0]);
        assertThrows(PermanentLockingException.class, () -> store[0].acquireLock(key, column, null, tx[0][0]));
    }

    /**
     * The check of the expected value takes BerkeleyJE's write lock of the key's record. Where a transaction which
     * didn't lock the key wrote it, the check fails once BerkeleyJE gives up waiting for that lock, and the
     * transaction can still roll back, which releases its locks.
     */
    @Test
    public void testCheckFailsOnTheRecordLockOfAnotherTransaction() throws BackendException {
        store[1].mutate(key, entry(value), NO_DELETIONS, tx[1][0]);
        store[0].acquireLock(key, column, null, tx[0][0]);
        ((BerkeleyJETx) tx[0][0]).getTransaction().setLockTimeout(100, TimeUnit.MILLISECONDS);

        final PermanentLockingException failure;
        try {
            failure = assertThrows(PermanentLockingException.class,
                () -> store[0].mutate(key, entry(otherValue), NO_DELETIONS, tx[0][0]));
        } finally {
            tx[0][0].rollback();
            tx[0][0] = newTransaction(manager[0]);
        }
        assertTrue(failure.getCause() instanceof LockConflictException, failure::toString);
        tx[1][0].commit();
        tx[1][0] = newTransaction(manager[1]);

        store[0].acquireLock(key, column, value, tx[0][0]);
        store[0].mutate(key, entry(otherValue), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();
        tx[0][0] = newTransaction(manager[0]);
        assertEquals(otherValue, KCVSUtil.get(store[1], key, column, tx[1][0]));
    }

    /**
     * The check reads the committed value of the key, also for a transaction which reads uncommitted values: it waits
     * for the transaction which writes the key, which here rolls back.
     */
    @Test
    public void testCheckReadsTheCommittedValue() throws Exception {
        final KeyColumnValueStoreManager uncommittedManager = openStorageManager(BerkeleyStorageSetup
            .getBerkeleyJEConfiguration().set(BerkeleyJEStoreManager.LOCK_MODE, LockMode.READ_UNCOMMITTED.toString()));
        final KeyColumnValueStore uncommittedStore = uncommittedManager.openDatabase(DB_NAME);
        final StoreTransaction reader = uncommittedManager.beginTransaction(getTxConfig());
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            store[1].mutate(key, entry(value), NO_DELETIONS, tx[1][0]);
            assertEquals(value, KCVSUtil.get(uncommittedStore, key, column, reader));

            uncommittedStore.acquireLock(key, column, null, reader);
            ((BerkeleyJETx) reader).getTransaction().setLockTimeout(10, TimeUnit.SECONDS);
            final Future<?> rollback = executor.submit(() -> {
                Thread.sleep(200);
                tx[1][0].rollback();
                return null;
            });
            try {
                uncommittedStore.mutate(key, entry(otherValue), NO_DELETIONS, reader);
            } finally {
                rollback.get(10, TimeUnit.SECONDS);
                tx[1][0] = newTransaction(manager[1]);
            }
            reader.commit();
            assertEquals(otherValue, KCVSUtil.get(store[0], key, column, tx[0][0]));
        } finally {
            //Not interrupted: BerkeleyJE invalidates its environment when a thread is interrupted in it
            executor.shutdown();
            reader.rollback();
            uncommittedStore.close();
            uncommittedManager.close();
        }
    }

    /**
     * A lock never waits, so it can't deadlock with BerkeleyJE's record locks: a transaction which holds a lock and
     * waits for the record lock of another transaction gets it once the other one fails to take that lock and rolls
     * back.
     */
    @Test
    public void testLockNeverWaitsForAnotherTransaction() throws Exception {
        final StaticBuffer recordKey = KeyValueStoreUtil.getBuffer("record");
        store[1].mutate(recordKey, entry(value), NO_DELETIONS, tx[1][0]);
        store[0].acquireLock(key, column, null, tx[0][0]);
        ((BerkeleyJETx) tx[0][0]).getTransaction().setLockTimeout(10, TimeUnit.SECONDS);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<?> write = executor.submit(() -> {
                store[0].mutate(recordKey, entry(otherValue), NO_DELETIONS, tx[0][0]);
                return null;
            });
            try {
                assertThrows(PermanentLockingException.class, () -> store[1].acquireLock(key, column, null, tx[1][0]));
            } finally {
                tx[1][0].rollback();
                tx[1][0] = newTransaction(manager[1]);
            }
            write.get(10, TimeUnit.SECONDS);
            tx[0][0].commit();
            tx[0][0] = newTransaction(manager[0]);
        } finally {
            //Not interrupted: BerkeleyJE invalidates its environment when a thread is interrupted in it
            executor.shutdown();
        }
        assertEquals(otherValue, KCVSUtil.get(store[1], recordKey, column, tx[1][0]));
    }

    /**
     * Without transactions, BerkeleyJE takes no locks, as before: two transactions both lock a key and write it.
     */
    @Test
    public void testNoLocksWithoutTransactions() throws BackendException {
        final KeyColumnValueStoreManager nonTransactional = openStorageManager(BerkeleyStorageSetup
            .getBerkeleyJEConfiguration(StorageSetup.getHomeDir("berkeleyje-no-transactions"))
            .set(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL, false));
        final KeyColumnValueStore nonTransactionalStore = nonTransactional.openDatabase(DB_NAME);
        try {
            final StoreTransaction first = nonTransactional.beginTransaction(getTxConfig());
            final StoreTransaction second = nonTransactional.beginTransaction(getTxConfig());
            nonTransactionalStore.acquireLock(key, column, null, first);
            nonTransactionalStore.acquireLock(key, column, null, second);
            nonTransactionalStore.mutate(key, entry(value), NO_DELETIONS, first);
            nonTransactionalStore.mutate(key, entry(otherValue), NO_DELETIONS, second);
            first.commit();
            second.commit();
            final StoreTransaction reader = nonTransactional.beginTransaction(getTxConfig());
            assertEquals(otherValue, KCVSUtil.get(nonTransactionalStore, key, column, reader));
            reader.commit();
        } finally {
            nonTransactionalStore.close();
            nonTransactional.clearStorage();
        }
    }

    /**
     * The transactions of a manager of another directory don't contend for the locks of this one.
     */
    @Test
    public void testManagerOfAnotherDirectoryHasLocksOfItsOwn() throws BackendException {
        final KeyColumnValueStoreManager elsewhere = openStorageManager(
            BerkeleyStorageSetup.getBerkeleyJEConfiguration(StorageSetup.getHomeDir("berkeleyje-elsewhere")));
        final KeyColumnValueStore elsewhereStore = elsewhere.openDatabase(DB_NAME);
        final StoreTransaction elsewhereTx = elsewhere.beginTransaction(getTxConfig());
        try {
            store[0].acquireLock(key, column, null, tx[0][0]);
            elsewhereStore.acquireLock(key, column, null, elsewhereTx);
            elsewhereStore.mutate(key, entry(value), NO_DELETIONS, elsewhereTx);
        } finally {
            elsewhereTx.rollback();
            elsewhereStore.close();
            elsewhere.clearStorage();
        }
    }
}
