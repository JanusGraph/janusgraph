package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.*;
import com.persistit.exception.PersistitException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.List;
import static com.thinkaurelius.titan.diskstorage.persistit.PersistitStoreManager.VOLUME_NAME;

/**
 * Persistit implicitly assigns units of work to transactions depending
 * on the thread being executed. Titan seems to look at units of work and
 * transactions as two separate elements, so there's some weird interface
 * instantiation stuff to make it work properly
 *
 * persistit userdocs
 *  http://akiban.github.com/persistit/docs/
 *
 * persistit examples
 *  https://github.com/akiban/persistit/tree/master/examples
 *
 * persistit javadoc:
 *  http://akiban.github.com/persistit/javadoc/
 *
 *  @todo: implement exchange pool
 */
public class PersistitKeyValueStore implements KeyValueStore {

    private static ByteBuffer getByteBuffer(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes, 0, bytes.length);
        return b;
    }

    private static byte[] getByteArray(ByteBuffer buffer) {
        int offset = buffer.arrayOffset();
        byte[] bytes = new byte[buffer.remaining() - offset];
        System.arraycopy(buffer.array(), offset, bytes, offset, bytes.length);
        return bytes;
    }

    private final String name;
    private final PersistitStoreManager storeManager;
    private final Persistit persistit;

    public PersistitKeyValueStore(String n, PersistitStoreManager mgr, Persistit db) throws StorageException {
        name = n;
        storeManager = mgr;
        persistit = db;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Clears the contents of this kv store
     */
    public void clear() throws StorageException {
        try {
            Exchange exchange = persistit.getExchange(VOLUME_NAME, name, true);
            exchange.removeTree();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    private static class KeysIterator implements RecordIterator<ByteBuffer> {

        final PersistitTransaction transaction;
        final Exchange exchange;
        Object nextKey = null;
        private boolean isClosed = false;

        public KeysIterator(PersistitTransaction tx, Exchange exchange) throws StorageException {
            transaction = tx;
            this.exchange = exchange;
            begin();
            getNextKey();
        }

        private void getNextKey() throws StorageException {
            synchronized (transaction) {
                transaction.assign();
                try {
                    if (exchange.hasNext()) {
                        exchange.next();
                        nextKey = getKey(exchange);
                    } else {
                        nextKey = null;
                    }
                } catch (PersistitException e) {
                    throw new PermanentStorageException(e);
                }
            }
        }

        private void begin() throws StorageException {
            synchronized (transaction) {
                transaction.assign();
                exchange.getKey().to(Key.BEFORE);
            }
        }

        @Override
        public boolean hasNext() {
            return nextKey != null;
        }

        @Override
        public ByteBuffer next() throws StorageException {
            if (nextKey == null) throw new NoSuchElementException();
            ByteBuffer returnKey = (ByteBuffer) nextKey;
            getNextKey();
            return returnKey;
        }

        @Override
        public void close() {
            synchronized (transaction) {
                transaction.releaseExchange(exchange);
                isClosed = true;
            }
        }

        @Override
        protected void finalize() {
            if (!isClosed) {
                //@todo: log if we have to do anything here
                close();
                isClosed = true;
            }
        }
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction tx) throws StorageException {
        return new KeysIterator((PersistitTransaction)tx, ((PersistitTransaction) tx).getExchange(name));
    }

    static void toKey(Exchange exchange, ByteBuffer key) {
        byte[] k = getByteArray(key);
        Key ek = exchange.getKey();
        ek.to(k);
    }

    static ByteBuffer getKey(Exchange exchange) {
        return getByteBuffer(exchange.getKey().decodeByteArray());
    }

    static void setValue(Exchange exchange, ByteBuffer val) throws PersistitException{
        byte[] v = getByteArray(val);
        exchange.getValue().put(v);
        
        exchange.store();
    }

    static ByteBuffer getValue(Exchange exchange) {
        byte[] dst = exchange.getValue().getByteArray();
        return ByteBuffer.wrap(dst);
    }

    @Override
    public ByteBuffer get(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        final PersistitTransaction tx = (PersistitTransaction) txh;
        synchronized (tx) {
            tx.assign();
            final Exchange exchange = tx.getExchange(name);

            try {
                toKey(exchange, key);
                exchange.fetch();
                if (exchange.getValue().isDefined()) {
                    return getValue(exchange);
                } else {
                    return null;
                }
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            } finally {
                tx.releaseExchange(exchange);
            }
        }
    }

    @Override
    public boolean containsKey(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        final PersistitTransaction tx = (PersistitTransaction) txh;
        synchronized (tx) {
            tx.assign();
            final Exchange exchange = tx.getExchange(name);
            try {
                toKey(exchange, key);
                return exchange.isValueDefined();
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            } finally {
                tx.releaseExchange(exchange);
            }
        }
    }

    /**
     * Compare 2 byte arrays, return 0 if equal, 1 if a > b, -1 if b > a
     */
    private int compare(final byte[] a, final byte[] b) {
        final int size = Math.min(a.length, b.length);
        for (int i = 0; i < size; i++) {
            if (a[i] != b[i]) {
                if ((a[i] & 0xFF) > (b[i] & 0xFF))
                    return 1;
                else
                    return -1;
            }
        }
        if (a.length < b.length)
            return -1;
        if (a.length > b.length)
            return 1;
        return 0;
    }

    /**
     * Runs all getSlice queries
     *
     * The keyStart & keyEnd are not guaranteed to exist
     * if keyStart is after keyEnd, an empty list is returned
     *
     * @param keyStart
     * @param keyEnd
     * @param selector
     * @param limit
     * @param txh
     * @return
     * @throws StorageException
     */
    private List<KeyValueEntry> getSlice(final ByteBuffer keyStart, final ByteBuffer keyEnd, final KeySelector selector, final Integer limit, StoreTransaction txh) throws StorageException {
        final PersistitTransaction tx = (PersistitTransaction) txh;
        synchronized (tx) {
            tx.assign();
            final Exchange exchange = tx.getExchange(name);

            try {
                ArrayList<KeyValueEntry> results = new ArrayList<KeyValueEntry>();

                byte[] start = getByteArray(keyStart);
                byte[] end = getByteArray(keyEnd);

                //bail out if the start key comes after the end
                if (compare(start, end) > 0) {
                    return results;
                }

                KeyFilter.Term[] terms = {KeyFilter.rangeTerm(start, end, true, false, null)};
                KeyFilter keyFilter = new KeyFilter(terms);

                int i = 0;
                while (exchange.next(keyFilter)) {
                    ByteBuffer k = getKey(exchange);
                    //check the key against the selector, and that is has a corresponding value
                    if (exchange.getValue().isDefined() && (selector == null || selector.include(k))){

                        ByteBuffer v = getValue(exchange);
                        KeyValueEntry kv = new KeyValueEntry(k, v);
                        results.add(kv);
                        i++;

                        if (limit != null && limit >= 0 && i >= limit) break;
                        if (selector != null && selector.reachedLimit()) break;
                    }
                }
                return results;
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            } finally {
                tx.releaseExchange(exchange);
            }
        }
    }

    @Override
    public List<KeyValueEntry> getSlice(final ByteBuffer keyStart, final ByteBuffer keyEnd, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, null, null, txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, KeySelector selector, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, selector, null, txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, null, limit, txh);
    }

    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value, final StoreTransaction txh) throws StorageException {
        final PersistitTransaction tx = (PersistitTransaction) txh;
        synchronized (tx) {
            tx.assign();
            final Exchange exchange = tx.getExchange(name);
            try {
                toKey(exchange, key);
                setValue(exchange, value);
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            } finally {
                tx.releaseExchange(exchange);
            }
        }
    }

    @Override
    public void delete(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        final PersistitTransaction tx = (PersistitTransaction) txh;
        synchronized (tx) {
            tx.assign();
            final Exchange exchange = tx.getExchange(name);
            try {
                toKey(exchange, key);
                exchange.remove();
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            } finally {
                tx.releaseExchange(exchange);
            }
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        //@todo: what is this supposed to do? BerkelyDB doesn't really implement this
    }

    @Override
    public void close() throws StorageException {
        storeManager.removeDatabase(this);
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }
}
