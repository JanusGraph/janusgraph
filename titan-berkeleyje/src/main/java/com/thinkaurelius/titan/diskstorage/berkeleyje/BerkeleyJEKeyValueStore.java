package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.LimitedSelector;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class BerkeleyJEKeyValueStore implements KeyValueStore {

    private Logger log = LoggerFactory.getLogger(BerkeleyJEKeyValueStore.class);

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
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        log.trace("Contains query");
        Transaction tx = getTransaction(txh);
        try {
            DatabaseEntry dbkey = getDataEntry(key);
            DatabaseEntry data = new DatabaseEntry();

            OperationStatus status = db.get(tx, dbkey, data, LockMode.DEFAULT);
            return status == OperationStatus.SUCCESS;

        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
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
    public ByteBuffer get(ByteBuffer key, StoreTransaction txh) throws StorageException {
        log.trace("Get query");
        Transaction tx = getTransaction(txh);
        try {
            DatabaseEntry dbkey = getDataEntry(key);
            DatabaseEntry data = new DatabaseEntry();

            OperationStatus status = db.get(tx, dbkey, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return getByteBuffer(data);
            } else {
                return null;
            }
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        log.trace("Acquiring lock.");
        if (getTransaction(txh) == null) {
//            throw new PermanentLockingException("Enable transaction for locking in BerkeleyDB!");
        } //else we need no locking
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, Integer.MAX_VALUE, txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, new LimitedSelector(limit), txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd,
                                        KeySelector selector, StoreTransaction txh) throws StorageException {
        log.trace("Get slice query");
        Transaction tx = getTransaction(txh);
        Cursor cursor = null;
        List<KeyValueEntry> result;
        try {
            //log.debug("Sta: {}",ByteBufferUtil.toBitString(keyStart, " "));
            //log.debug("Head: {}",ByteBufferUtil.toBitString(keyEnd, " "));

            DatabaseEntry foundKey = getDataEntry(keyStart);
            DatabaseEntry foundData = new DatabaseEntry();

            cursor = db.openCursor(tx, null);
            OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT);
            result = new ArrayList<KeyValueEntry>();
            //Iterate until given condition is satisfied or end of records
            while (status == OperationStatus.SUCCESS) {

                ByteBuffer key = getByteBuffer(foundKey);
                //log.debug("Fou: {}",ByteBufferUtil.toBitString(nextKey, " "));
                //keyEnd.rewind();
                if (ByteBufferUtil.compare(key, keyEnd)>=0) break;
                //nextKey.rewind();

                boolean skip = false;

                if (!skip) {
                    skip = !selector.include(key);

                    if (!skip) {
                        result.add(new KeyValueEntry(key, getByteBuffer(foundData)));
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

    private static class KeysIterator implements RecordIterator<ByteBuffer> {

        final StoreTransaction txh;
        Cursor cursor;
        final DatabaseEntry foundValue;
        final DatabaseEntry foundKey;

        ByteBuffer nextKey;

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
                    nextKey = getByteBuffer(foundKey);
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
                    nextKey = getByteBuffer(foundKey);
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
        public ByteBuffer next() throws StorageException {
            if (nextKey == null) throw new NoSuchElementException();
            ByteBuffer returnKey = nextKey;
            getNextKey();
            return returnKey;
        }

    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(final StoreTransaction txh) throws StorageException {
        log.trace("Get keys iterator");
        KeysIterator iterator = new KeysIterator(txh, db);
        return iterator;
    }

    @Override
    public void insert(ByteBuffer key, ByteBuffer value, StoreTransaction txh) throws StorageException {
        Transaction tx = getTransaction(txh);
        insert(key, value, tx, true);
    }

    public void insert(ByteBuffer key, ByteBuffer value, Transaction tx, boolean allowOverwrite) throws StorageException {
        try {
            //log.debug("Key: {}",ByteBufferUtil.toBitString(entry.getKey(), " "));
            OperationStatus status = null;
            if (allowOverwrite)
                status = db.put(tx, getDataEntry(key), getDataEntry(value));
            else
                status = db.putNoOverwrite(tx, getDataEntry(key), getDataEntry(value));

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
    public void delete(ByteBuffer key, StoreTransaction txh) throws StorageException {
        log.trace("Deletion");
        Transaction tx = getTransaction(txh);
        try {
            OperationStatus status = db.delete(tx, getDataEntry(key));
            if (status != OperationStatus.SUCCESS) {
                throw new PermanentStorageException("Could not remove: " + status);
            }
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }


    private final static DatabaseEntry getDataEntry(ByteBuffer key) {
        assert key.position() == 0;
        DatabaseEntry dbkey = new DatabaseEntry(key.array(), key.arrayOffset(), key.arrayOffset() + key.remaining());
        return dbkey;
    }

    private final static ByteBuffer getByteBuffer(DatabaseEntry entry) {
        ByteBuffer buffer = ByteBuffer.wrap(entry.getData(), entry.getOffset(), entry.getSize());
        buffer.rewind();
        return buffer;
    }


}
