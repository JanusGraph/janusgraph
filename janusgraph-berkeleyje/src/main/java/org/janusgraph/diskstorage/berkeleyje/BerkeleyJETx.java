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

package org.janusgraph.diskstorage.berkeleyje;

import com.google.common.base.Preconditions;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BerkeleyJETx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJETx.class);

    private volatile Transaction tx;
    private volatile boolean isOpen;
    private final List<Cursor> openCursors = new ArrayList<>();
    private final LockMode lockMode;
    private final CacheMode cacheMode;
    private final List<Lock> locks = new ArrayList<>();
    private volatile boolean mutationStarted;

    public BerkeleyJETx(Transaction t, LockMode lockMode, CacheMode cacheMode, BaseTransactionConfig config) {
        super(config);
        tx = t;
        this.lockMode = lockMode;
        this.cacheMode = cacheMode;
        isOpen = true;
        // tx may be null
        Preconditions.checkNotNull(this.lockMode);
    }

    public Transaction getTransaction() {
        return tx;
    }

    Cursor openCursor(Database db) throws BackendException {
        synchronized (openCursors) {
            if (!isOpen) {
                throw new PermanentBackendException("Transaction already closed");
            }
            Cursor cursor = db.openCursor(tx, null);
            openCursors.add(cursor);
            return cursor;
        }
    }

    void closeCursor(Cursor cursor) {
        synchronized (openCursors) {
            cursor.close();
            openCursors.remove(cursor);
        }
    }

    private void closeOpenCursors() {
        synchronized (openCursors) {
            openCursors.forEach(Cursor::close);
        }
    }

    CacheMode getCacheMode() {
        return cacheMode;
    }

    LockMode getLockMode() {
        return lockMode;
    }

    /**
     * Takes the lock of the key for this transaction, unless it holds it already, and keeps the value which the key is
     * expected to have, which the first mutation checks.
     *
     * @throws PermanentLockingException if another transaction holds the lock, or this one has started its mutations
     */
    synchronized void lock(BerkeleyJEKeyValueStore store, StaticBuffer key, StaticBuffer expectedValue)
            throws BackendException {
        if (mutationStarted) {
            throw new PermanentLockingException("Attempted to obtain a lock after mutations had been persisted");
        }
        if (store.lock(key, this)) {
            locks.add(new Lock(store, key, expectedValue));
        }
    }

    /**
     * Checks the expected values of the locked keys before the first mutation of this transaction. The keys stay as
     * they are checked until the transaction ends: their locks keep out other transactions which lock them, and the
     * check takes BerkeleyJE's write locks of the records which exist.
     */
    void prepareForMutations() throws BackendException {
        if (mutationStarted) return;
        synchronized (this) {
            if (mutationStarted) return;
            for (Lock lock : locks) {
                lock.store.checkExpectedValue(lock.key, lock.expectedValue, this);
            }
            mutationStarted = true;
        }
    }

    //Once BerkeleyJE has committed or aborted the transaction, or failed to, so that the next holder of a lock reads
    //the outcome
    private void releaseLocks() {
        for (Lock lock : locks) {
            lock.store.unlock(lock.key, this);
        }
        locks.clear();
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this, new TransactionClose(this.toString()));
        try {
            isOpen = false;
            closeOpenCursors();
            tx.abort();
            tx = null;
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        } finally {
            releaseLocks();
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} committed", this, new TransactionClose(this.toString()));
        try {
            isOpen = false;
            closeOpenCursors();
            tx.commit();
            tx = null;
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        } finally {
            releaseLocks();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static final class Lock {
        private final BerkeleyJEKeyValueStore store;
        private final StaticBuffer key;
        private final StaticBuffer expectedValue;

        private Lock(BerkeleyJEKeyValueStore store, StaticBuffer key, StaticBuffer expectedValue) {
            this.store = store;
            this.key = key;
            this.expectedValue = expectedValue;
        }
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}
