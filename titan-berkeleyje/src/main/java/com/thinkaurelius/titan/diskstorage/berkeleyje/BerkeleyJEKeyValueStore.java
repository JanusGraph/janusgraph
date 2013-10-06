package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BerkeleyJEKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEKeyValueStore.class);

    private static final StaticBuffer.Factory<DatabaseEntry> ENTRY_FACTORY = new StaticBuffer.Factory<DatabaseEntry>() {
        @Override
        public DatabaseEntry get(byte[] array, int offset, int limit) {
            return new DatabaseEntry(array,offset,limit-offset);
        }
    };


    private final Database db;
    private final String name;
    private final BerkeleyJEStoreManager manager;

    public BerkeleyJEKeyValueStore(String n, Database data, BerkeleyJEStoreManager m) {
        db = data;
        name = n;
        manager = m;
    }

    public DatabaseConfig getConfiguration() throws StorageException {
        try {
            return db.getConfig();
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static final Transaction getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh!=null);
        return ((BerkeleyJETx) txh).getTransaction();
    }

    @Override
    public void close() throws StorageException {
        try {
            db.close();
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
        manager.removeDatabase(this);
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException {
        Transaction tx = getTransaction(txh);
        try {
            DatabaseEntry dbkey = key.as(ENTRY_FACTORY);
            DatabaseEntry data = new DatabaseEntry();

            OperationStatus status = db.get(tx, dbkey, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return getBuffer(data);
            } else {
                return null;
            }
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        if (getTransaction(txh) == null) {
            log.info("Attempt to acquire lock with transactions disabled");
        } //else we need no locking
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(StaticBuffer keyStart, StaticBuffer keyEnd,
                                                  KeySelector selector, StoreTransaction txh) throws StorageException {
        log.trace("Get slice query");
        Transaction tx = getTransaction(txh);
        Cursor cursor = null;
        final List<KeyValueEntry> result = new ArrayList<KeyValueEntry>();
        try {
            DatabaseEntry foundKey = keyStart.as(ENTRY_FACTORY);
            DatabaseEntry foundData = new DatabaseEntry();

            cursor = db.openCursor(tx, null);
            OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT);
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

                status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
            }
            log.trace("Retrieved: {}", result.size());

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
            throw new PermanentStorageException(e);
        } finally {
            try {
                if (cursor != null) cursor.close();
            } catch (Exception e) {
                throw new PermanentStorageException(e);
            }
        }
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws StorageException {
        Transaction tx = getTransaction(txh);
        insert(key, value, tx, true);
    }

    public void insert(StaticBuffer key, StaticBuffer value, Transaction tx, boolean allowOverwrite) throws StorageException {
        try {
            OperationStatus status;
            if (allowOverwrite)
                status = db.put(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
            else
                status = db.putNoOverwrite(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));

            if (status != OperationStatus.SUCCESS) {
                if (status == OperationStatus.KEYEXIST) {
                    throw new PermanentStorageException("Key already exists on no-overwrite.");
                } else {
                    throw new PermanentStorageException("Could not write entity, return status: " + status);
                }
            }
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException {
        log.trace("Deletion");
        Transaction tx = getTransaction(txh);
        try {
            OperationStatus status = db.delete(tx, key.as(ENTRY_FACTORY));
            if (status != OperationStatus.SUCCESS) {
                throw new PermanentStorageException("Could not remove: " + status);
            }
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }

    private static StaticBuffer getBuffer(DatabaseEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }


}
