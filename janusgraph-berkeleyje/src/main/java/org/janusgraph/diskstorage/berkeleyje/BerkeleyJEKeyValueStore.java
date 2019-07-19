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
import com.sleepycat.je.*;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BerkeleyJEKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEKeyValueStore.class);

    private static final StaticBuffer.Factory<DatabaseEntry> ENTRY_FACTORY = (array, offset, limit) -> new DatabaseEntry(array,offset,limit-offset);


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

            OperationStatus status = db.get(tx, databaseKey, data, getLockMode(txh));

            if (status == OperationStatus.SUCCESS) {
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
        final List<KeyValueEntry> result = new ArrayList<>();
        final DatabaseEntry foundKey = keyStart.as(ENTRY_FACTORY);
        final DatabaseEntry foundData = new DatabaseEntry();

        final Cursor cursor = openCursor(txh);
        try {
            OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, getLockMode(txh));
            //Iterate until given condition is satisfied or end of records
            while (status == OperationStatus.SUCCESS) {
                StaticBuffer key = getBuffer(foundKey);

                if (key.compareTo(keyEnd) >= 0)
                    break;

                if (selector.include(key)) {
                    result.add(new KeyValueEntry(key, getBuffer(foundData)));
                }

                if (selector.reachedLimit())
                    break;

                status = cursor.getNext(foundKey, foundData, getLockMode(txh));
            }
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {
            closeCursor(txh, cursor);
        }
        log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, result.size());

        return new RecordIterator<KeyValueEntry>() {
            private final Iterator<KeyValueEntry> entries = result.iterator();

            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public KeyValueEntry next() {
                return entries.next();
            }

            @Override
            public void close() {
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
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        insert(key, value, txh, true);
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, boolean allowOverwrite) throws BackendException {
        Transaction tx = getTransaction(txh);
        try {
            OperationStatus status;

            log.trace("db={}, op=insert, tx={}", name, txh);

            if (allowOverwrite)
                status = db.put(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
            else
                status = db.putNoOverwrite(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));

            if (status != OperationStatus.SUCCESS) {
                if (status == OperationStatus.KEYEXIST) {
                    throw new PermanentBackendException("Key already exists on no-overwrite.");
                } else {
                    throw new PermanentBackendException("Could not write entity, return status: " + status);
                }
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        log.trace("Deletion");
        Transaction tx = getTransaction(txh);
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            OperationStatus status = db.delete(tx, key.as(ENTRY_FACTORY));
            if (status != OperationStatus.SUCCESS) {
                throw new PermanentBackendException("Could not remove: " + status);
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }

    private static StaticBuffer getBuffer(DatabaseEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }

    private static LockMode getLockMode(StoreTransaction txh) {
        return ((BerkeleyJETx)txh).getLockMode();
    }
}
