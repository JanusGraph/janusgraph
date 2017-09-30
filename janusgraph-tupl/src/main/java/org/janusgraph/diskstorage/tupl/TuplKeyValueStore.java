/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.janusgraph.diskstorage.tupl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.cojen.tupl.*;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * TuplKeyValueStore is the KV Store implementation for the Classmethod Storage Backend for Titan
 * @author Alexander Patrikalakis
 *
 */
public class TuplKeyValueStore implements OrderedKeyValueStore {
    private static final Logger log = LoggerFactory.getLogger(TuplKeyValueStore.class);
    private final String name;
    private final Index dbindex;
    private final TuplStoreManager manager;

    /**
     * Constructs a new KVStore for the Tupl backend
     * @param name name of the kvstore (graphindex, tx_log etc)
     * @param dbindex the underlying index
     * @param mgr store manager that contains this store
     */
    TuplKeyValueStore(String name, Index dbindex, TuplStoreManager mgr) {
        this.name = name;
        this.dbindex = dbindex;
        this.manager = mgr;
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        final TuplStoreTransaction tx = TuplStoreTransaction.getTx(txh);
        try {
            //store does not condition on existing value
            StaticBuffer expectedValue = tx.getExpectedValue(name, dbindex.getId(), key);
            if(expectedValue == null) {
                dbindex.delete(tx.getTuplTxn(), getByteArray(key));
            } else if(!dbindex.remove(tx.getTuplTxn(), getByteArray(key) /*key*/, getByteArray(expectedValue))) {
                throw new PermanentLockingException(String.format("Remove: expected value did not match actual for key %s", key));
            }
        } catch (IOException e) {
            throw new PermanentBackendException("unable to delete key " + key, e);
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        final TuplStoreTransaction tx = TuplStoreTransaction.getTx(txh);
        final byte[] value;
        try {
            value = dbindex.load(tx.getTuplTxn(), getByteArray(key));
        } catch (IOException e) {
            throw new PermanentBackendException("unable to get key" + key, e);
        }
        return getBuffer(value);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh)
            throws BackendException {
        final TuplStoreTransaction tx = TuplStoreTransaction.getTx(txh);
        tx.expectValue(this.name, dbindex.getId(), key, expectedValue);
    }

    public String getName() {
        return name;
    }

    public synchronized void close() throws BackendException {
        try {
            dbindex.close();
        } catch (IOException e) {
            throw new PermanentBackendException("unable to close store named " + name, e);
        }
        manager.unregisterStore(this);
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        final TuplStoreTransaction tx = TuplStoreTransaction.getTx(txh);
        log.trace("insert id:{} index:{} name:{} key:{} value:{}", tx.getId(), dbindex.getId(), name, key, value);
        try {
            //store does not condition on existing value
            StaticBuffer expectedValue = tx.getExpectedValue(name, dbindex.getId(), key);
            if(expectedValue == null) {
                //blind insert
                dbindex.insert(tx.getTuplTxn(), getByteArray(key), getByteArray(value));
            } else if(!dbindex.update(tx.getTuplTxn(),
                    getByteArray(key), //key
                    getByteArray(expectedValue), //old expected value
                    getByteArray(value))) { //new value
                throw new PermanentLockingException(String.format("Expected value did not match actual for key:%s", key));
            }
        } catch (IOException e) {
            throw new PermanentBackendException("unable to close store named "+ name, e);
        }
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        final TuplStoreTransaction tx = TuplStoreTransaction.getTx(txh);
        final StaticBuffer start = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();
        final List<KeyValueEntry> result = new ArrayList<>();
        final Transaction txn = tx.getTuplTxn();

        try {
            final Cursor cursor = dbindex.viewGe(getByteArray(start)).newCursor(txn);
            LockResult status = cursor.first(); //TODO(amcp) determine if the status is necessary here?
            //Iterate until given condition is satisfied or end of records
            while (true) { //TODO(amcp) find a better solution
                StaticBuffer key = getBuffer(cursor.key());

                if (key == null || key.compareTo(keyEnd) >= 0) {
                    break;
                }

                if (selector.include(key)) {
                    result.add(new KeyValueEntry(key, getBuffer(cursor.value())));
                }

                if (selector.reachedLimit()) {
                    break;
                }

                status = cursor.next();
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
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh)
            throws BackendException {
        throw new UnsupportedOperationException();
    }

    private static StaticBuffer getBuffer(byte[] entry) {
        return entry == null ? null : StaticArrayBuffer.of(entry);
    }
    
    private static byte[] getByteArray(StaticBuffer buffer) {
        return buffer == null ? null : buffer.as(StaticBuffer.ARRAY_FACTORY);
    }
}
