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
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
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
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}
