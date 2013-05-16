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
    public List<KeyValueEntry> getSlice(StaticBuffer keyStart, StaticBuffer keyEnd,
                                        KeySelector selector, StoreTransaction txh) throws StorageException {
        log.trace("Get slice query");
        Transaction tx = getTransaction(txh);
        Cursor cursor = null;
        List<KeyValueEntry> result;
        try {
            //log.debug("Sta: {}",ByteBufferUtil.toBitString(keyStart, " "));
            //log.debug("Head: {}",ByteBufferUtil.toBitString(keyEnd, " "));

            DatabaseEntry foundKey = keyStart.as(ENTRY_FACTORY);
            DatabaseEntry foundData = new DatabaseEntry();

            cursor = db.openCursor(tx, null);
            OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT);
            result = new ArrayList<KeyValueEntry>();
            //Iterate until given condition is satisfied or end of records
            while (status == OperationStatus.SUCCESS) {

                StaticBuffer key = getBuffer(foundKey);
                //log.debug("Fou: {}",ByteBufferUtil.toBitString(nextKey, " "));
                //keyEnd.rewind();
                if (ByteBufferUtil.compare(key, keyEnd)>=0) break;
                //nextKey.rewind();

                boolean skip = false;

                if (!skip) {
                    skip = !selector.include(key);

                    if (!skip) {
                        result.add(new KeyValueEntry(key, getBuffer(foundData)));
                    }
                }
                if (selector.reachedLimit()) {
                    break;
                }
                status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
            }
            log.trace("Retrieved: {}", result.size());
            return result;
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

    private static class KeysIterator implements RecordIterator<StaticBuffer> {

        final StoreTransaction txh;
        Cursor cursor;
        final DatabaseEntry foundValue;
        final DatabaseEntry foundKey;

        StaticBuffer nextKey;

        public KeysIterator(StoreTransaction txh, Database db) throws StorageException {
            this.txh = txh;
            foundKey = new DatabaseEntry();
            foundValue = new DatabaseEntry();
            foundValue.setPartial(0, 0, true);
            cursor = null;

            //Register with transaction handle


            try {
                cursor = db.openCursor(getTransaction(txh), null);
                ((BerkeleyJETx) txh).registerCursor(cursor);
                OperationStatus status = cursor.getFirst(foundKey, foundValue, LockMode.DEFAULT);
                if (status == OperationStatus.SUCCESS) {
                    nextKey = getBuffer(foundKey);
                } else {
                    nextKey = null;
                    close();
                }
            } catch (Exception e) {
                close();
                throw new PermanentStorageException(e);
            }
        }

        @Override
        public void close() throws StorageException {
            try {
                if (cursor != null) cursor.close();
            } catch (Exception e) {
                throw new PermanentStorageException(e);
            }
        }

        private void getNextKey() throws StorageException {
            try {
                OperationStatus status = cursor.getNext(foundKey, foundValue, LockMode.DEFAULT);
                if (status == OperationStatus.SUCCESS) {
                    nextKey = getBuffer(foundKey);
                } else {
                    nextKey = null;
                    close();
                }
            } catch (Exception e) {
                close();
                throw new PermanentStorageException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return nextKey != null;
        }

        @Override
        public StaticBuffer next() throws StorageException {
            if (nextKey == null) throw new NoSuchElementException();
            StaticBuffer returnKey = nextKey;
            getNextKey();
            return returnKey;
        }

    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(final StoreTransaction txh) throws StorageException {
        log.trace("Get keys iterator");
        KeysIterator iterator = new KeysIterator(txh, db);
        return iterator;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws StorageException {
        Transaction tx = getTransaction(txh);
        insert(key, value, tx, true);
    }

    public void insert(StaticBuffer key, StaticBuffer value, Transaction tx, boolean allowOverwrite) throws StorageException {
        try {
            //log.debug("Key: {}",ByteBufferUtil.toBitString(entry.getKey(), " "));
            OperationStatus status = null;
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


//    private final static DatabaseEntry getDataEntry(ByteBuffer key) {
//        DatabaseEntry dbkey = new DatabaseEntry(key.array(), key.arrayOffset(), key.arrayOffset() + key.remaining());
//        return dbkey;
//    }
//
//    private final static ByteBuffer getByteBuffer(DatabaseEntry entry) {
//        ByteBuffer buffer = ByteBuffer.wrap(entry.getData(), entry.getOffset(), entry.getSize());
//        buffer.rewind();
//        return buffer;
//    }

    private final static StaticBuffer getBuffer(DatabaseEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }


}
