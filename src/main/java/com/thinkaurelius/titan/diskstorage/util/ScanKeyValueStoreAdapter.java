package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.ScanKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ScanKeyValueStoreAdapter extends OrderedKeyValueStoreAdapter implements ScanKeyColumnValueStore {

    protected final ScanKeyValueStore store;

    public ScanKeyValueStoreAdapter(ScanKeyValueStore store) {
        super(store);
        this.store=store;
    }

    public ScanKeyValueStoreAdapter(ScanKeyValueStore store, int keyLength) {
        super(store, keyLength);
        this.store=store;
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(TransactionHandle txh) throws StorageException {
        return new KeysIterator(store.getKeys(txh));
    }

    private class KeysIterator implements RecordIterator<ByteBuffer> {

        final RecordIterator<ByteBuffer> iterator;
        ByteBuffer nextKey;
        
        private KeysIterator(RecordIterator<ByteBuffer> iterator) throws StorageException {
            this.iterator=iterator;
            this.nextKey=null;
            getNextKey();
        }
        
        private void getNextKey() throws StorageException {
            boolean foundNextKey = false;
            while (!foundNextKey && iterator.hasNext()) {
                ByteBuffer keycolumn = iterator.next();
                if (nextKey==null || !equalKey(keycolumn,nextKey)) {
                    foundNextKey=true;
                    nextKey = getKey(keycolumn);
                }
            }
            if (!foundNextKey) nextKey=null;
        }

        @Override
        public boolean hasNext() throws StorageException {
            return nextKey!=null;
        }

        @Override
        public ByteBuffer next() throws StorageException {
            if (nextKey==null) throw new NoSuchElementException();
            ByteBuffer returnKey = nextKey;
            getNextKey();
            return returnKey;
        }

        @Override
        public void close() throws StorageException {
            iterator.close();
        }
    }
}
