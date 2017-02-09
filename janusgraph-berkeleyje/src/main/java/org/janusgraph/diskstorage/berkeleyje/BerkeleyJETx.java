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
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BerkeleyJETx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJETx.class);

    private volatile Transaction tx;
    private final List<Cursor> openCursors = new ArrayList<Cursor>();
    private final LockMode lm;

    public BerkeleyJETx(Transaction t, LockMode lockMode, BaseTransactionConfig config) {
        super(config);
        tx = t;
        lm = lockMode;
        // tx may be null
        Preconditions.checkNotNull(lm);
    }

    public Transaction getTransaction() {
        return tx;
    }

    void registerCursor(Cursor cursor) {
        Preconditions.checkArgument(cursor != null);
        synchronized (openCursors) {
            //TODO: attempt to remove closed cursors if there are too many
            openCursors.add(cursor);
        }
    }

    private void closeOpenIterators() throws BackendException {
        for (Cursor cursor : openCursors) {
            cursor.close();
        }
    }

    LockMode getLockMode() {
        return lm;
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
        try {
            closeOpenIterators();
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
            log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));

        try {
            closeOpenIterators();
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
