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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class BerkeleyJEKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEKeyValueStore.class);

    private static final StaticBuffer.Factory<DatabaseEntry> ENTRY_FACTORY = (array, offset, limit) -> new DatabaseEntry(array,offset,limit-offset);

    @VisibleForTesting
    public static Function<Integer, Integer> ttlConverter = ttl -> (int) Math.max(1, Duration.of(ttl, ChronoUnit.SECONDS).toHours());


    private final Database db;
    private final String name;
    private final BerkeleyJEStoreManager manager;
    private boolean isOpen;

    public BerkeleyJEKeyValueStore(String n, Database data, BerkeleyJEStoreManager m) {
        db = data;
        name = n;
        manager = m;
        isOpen = true;
    }

    public DatabaseConfig getConfiguration() throws BackendException {
        try {
            return db.getConfig();
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static Transaction getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh!=null);
        return ((BerkeleyJETx) txh).getTransaction();
    }

    private Cursor openCursor(StoreTransaction txh) throws BackendException {
        Preconditions.checkArgument(txh!=null);
        return ((BerkeleyJETx) txh).openCursor(db);
    }

    private static void closeCursor(StoreTransaction txh, Cursor cursor) {
        Preconditions.checkArgument(txh!=null);
        ((BerkeleyJETx) txh).closeCursor(cursor);
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            if(isOpen) db.close();
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        Transaction tx = getTransaction(txh);
        try {
            DatabaseEntry databaseKey = key.as(ENTRY_FACTORY);
            DatabaseEntry data = new DatabaseEntry();

            log.trace("db={}, op=get, tx={}", name, txh);

            OperationResult result = db.get(tx, databaseKey, data, Get.SEARCH, getReadOptions(txh));

            if (result != null) {
                return getBuffer(data);
            } else {
                return null;
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            log.warn("Attempt to acquire lock with transactions disabled");
        } //else we need no locking
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        log.trace("beginning db={}, op=getSlice, tx={}", name, txh);
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();
        final DatabaseEntry foundKey = keyStart.as(ENTRY_FACTORY);
        final DatabaseEntry foundData = new DatabaseEntry();
        final Cursor cursor = openCursor(txh);

        return new RecordIterator<KeyValueEntry>() {
            private OperationStatus status;
            private KeyValueEntry current;

            @Override
            public boolean hasNext() {
                if (current == null) {
                    current = getNextEntry();
                }
                return current != null;
            }

            @Override
            public KeyValueEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                KeyValueEntry next = current;
                current = null;
                return next;
            }

            private KeyValueEntry getNextEntry() {
                if (status != null && status != OperationStatus.SUCCESS) {
                    return null;
                }
                while (!selector.reachedLimit()) {
                    if (status == null) {
                        status = cursor.get(foundKey, foundData, Get.SEARCH_GTE, getReadOptions(txh)) == null ? OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
                    } else {
                        status = cursor.get(foundKey, foundData, Get.NEXT, getReadOptions(txh)) == null ? OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
                    }
                    if (status != OperationStatus.SUCCESS) {
                        break;
                    }
                    StaticBuffer key = getBuffer(foundKey);

                    if (key.compareTo(keyEnd) >= 0) {
                        status = OperationStatus.NOTFOUND;
                        break;
                    }

                    if (selector.include(key)) {
                        return new KeyValueEntry(key, getBuffer(foundData));
                    }
                }
                return null;
            }

            @Override
            public void close() {
                closeCursor(txh, cursor);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        insert(key, value, txh, true, ttl);
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, boolean allowOverwrite, Integer ttl) throws BackendException {
        Transaction tx = getTransaction(txh);
        OperationStatus status;

        log.trace("db={}, op=insert, tx={}", name, txh);

        WriteOptions writeOptions = getWriteOptions(txh);

        if (ttl != null && ttl > 0) {
            int convertedTtl = ttlConverter.apply(ttl);
            writeOptions.setTTL(convertedTtl, TimeUnit.HOURS);
        }
        if (allowOverwrite) {
            OperationResult result = db.put(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY), Put.OVERWRITE, writeOptions);
            EnvironmentFailureException.assertState(result != null);
            status = OperationStatus.SUCCESS;
        } else {
            OperationResult result = db.put(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY), Put.NO_OVERWRITE, writeOptions);
            status = result == null ? OperationStatus.KEYEXIST : OperationStatus.SUCCESS;
        }

        if (status != OperationStatus.SUCCESS) {
            throw new PermanentBackendException("Key already exists on no-overwrite.");
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        log.trace("Deletion");
        Transaction tx = getTransaction(txh);
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            OperationStatus status = db.delete(tx, key.as(ENTRY_FACTORY));
            if (status != OperationStatus.SUCCESS && status != OperationStatus.NOTFOUND) {
                throw new PermanentBackendException("Could not remove: " + status);
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }

    private static StaticBuffer getBuffer(DatabaseEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }

    private WriteOptions getWriteOptions(final StoreTransaction txh) {
        return new WriteOptions().setCacheMode(((BerkeleyJETx) txh).getCacheMode());
    }

    private ReadOptions getReadOptions(final StoreTransaction txh) {
        return new ReadOptions().setCacheMode(((BerkeleyJETx) txh).getCacheMode())
                                .setLockMode(((BerkeleyJETx) txh).getLockMode());
    }
}
