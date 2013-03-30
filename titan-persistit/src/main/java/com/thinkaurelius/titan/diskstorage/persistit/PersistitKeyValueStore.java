package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.*;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
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

    /**
     * extension of TransactionRunnable that runs operations on an exchange
     * in the context of an arbitrary transaction and provides a method for
     * getting the result of the job
     */
    static abstract class PersistitJob implements TransactionRunnable {
        Object result = null;
        Exchange exchange = null;

        public void setExchange(Exchange exchange) {
            this.exchange = exchange;
        }

        public Exchange getExchange() {
            return exchange;
        }

        /**
         * Returns the result of the job
         * @return
         */
        public Object getResult() {
            return result;
        }
    }

    private static ByteBuffer getByteBuffer(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes, 0, bytes.length);
        b.rewind();
        return b;
    }

    private static byte[] getByteArray(ByteBuffer buffer) {
        buffer.rewind();
        return buffer.array();
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
            Exchange exchange = persistit.getExchange(persistit.getSystemVolume().getName(), name, true);
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

        private PersistitJob getNextKeyJob;

        public KeysIterator(PersistitTransaction tx, Exchange exchange) throws StorageException {
            transaction = tx;
            this.exchange = exchange;

            getNextKeyJob = new PersistitJob() {
                @Override
                public void runTransaction() throws PersistitException, RollbackException {
                    if (exchange.hasNext()) {
                        exchange.next();
//                        result = getByteBuffer(exchange.getKey().decodeByteArray());
                        result = getKey(exchange);
                    } else {
                        result = null;
                    }
                }
            };
            getNextKeyJob.setExchange(exchange);

            begin();
            getNextKey();
        }

        private void getNextKey() throws StorageException {
            transaction.run(getNextKeyJob);
            nextKey = getNextKeyJob.getResult();
        }

        private void begin() throws StorageException {
            PersistitJob j = new PersistitJob() {
                @Override
                public void runTransaction() throws PersistitException, RollbackException {
                    exchange.getKey().to(Key.BEFORE);
                }
            };

            j.setExchange(exchange);
            transaction.run(j);
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
            transaction.releaseExchange(exchange);
            isClosed = true;
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
//        exchange.getKey().to(new String(key.array()));
        byte[] k = getByteArray(key);
        Key ek = exchange.getKey();
        ek.clear();
        ek.appendByteArray(k, 0, k.length);
    }

    static ByteBuffer getKey(Exchange exchange) {
        return getByteBuffer(exchange.getKey().decodeByteArray());
//        return getByteBuffer(exchange.getKey().decodeString().getBytes());
    }

    static void setValue(Exchange exchange, ByteBuffer val) throws PersistitException{
//        exchange.getValue().put(new String(val.array()));
        byte[] v = getByteArray(val);
        Value ev = exchange.getValue();
        ev.clear();
        ev.putByteArray(v, 0, v.length);

        exchange.store();
    }

    static ByteBuffer getValue(Exchange exchange) {
//        byte[] bytes = exchange.getValue().getString().getBytes();
//        return ByteBuffer.wrap(bytes);
        return getByteBuffer(exchange.getValue().getByteArray());
    }

    @Override
    public ByteBuffer get(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                toKey(exchange, key);

                exchange.fetch();
                if (exchange.getValue().isDefined()) {
                    result = getValue(exchange);
                } else {
                    result = null;
                }
            }
        };
        final PersistitTransaction tx = (PersistitTransaction) txh;
        final Exchange exchange = tx.getExchange(name);
        try {
            j.setExchange(exchange);
            tx.run(j);
        } finally {
            tx.releaseExchange(exchange);
        }
        return (ByteBuffer) j.getResult();
    }

    @Override
    public boolean containsKey(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                toKey(exchange, key);
                result = exchange.isValueDefined();
            }
        };
        final PersistitTransaction tx = (PersistitTransaction) txh;
        final Exchange exchange = tx.getExchange(name);
        try {
            j.setExchange(exchange);
            tx.run(j);
        } finally {
            tx.releaseExchange(exchange);
        }
        return (Boolean) j.getResult();
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
        PersistitJob j = new PersistitJob() {

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
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                ArrayList<KeyValueEntry> results = new ArrayList<KeyValueEntry>();

                byte[] start = getByteArray(keyStart);
                byte[] end = getByteArray(keyEnd);

                //bail out if the start key comes after the end
                if (compare(start, end) > 0) {
                    result = results;
                    return;
                }

                KeyFilter.Term[] terms = {KeyFilter.rangeTerm(start, end, true, false, null)};
//                KeyFilter.Term[] terms = {KeyFilter.rangeTerm(new String(start), new String(), true, false, null)};
                KeyFilter keyFilter = new KeyFilter(terms);

                toKey(exchange, keyStart);
                exchange.fetch();

                int i = 0;
                while (keyFilter.selected(exchange.getKey())) {
                    ByteBuffer k = getKey(exchange);
                    exchange.fetch();

                    //check the key against the selector, and that is has a corresponding value
                    if (exchange.getValue().isDefined() && (selector == null || selector.include(k))){
                        if (limit != null && limit >= 0 && i >= limit) break;

                        ByteBuffer v = getValue(exchange);
                        KeyValueEntry kv = new KeyValueEntry(k, v);
                        results.add(kv);
                        i++;

                        if (selector != null && selector.reachedLimit()) break;
                    }
                    if (exchange.hasNext()) {
                        exchange.next();
                    } else {
                        break;
                    }
                }
                result = results;
            }
        };

        final PersistitTransaction tx = (PersistitTransaction) txh;
        final Exchange exchange = tx.getExchange(name);
        try {
            j.setExchange(exchange);
            tx.run(j);
        } finally {
            tx.releaseExchange(exchange);
        }
        return (List<KeyValueEntry>) j.getResult();
    }

    @Override
    public List<KeyValueEntry> getSlice(final ByteBuffer keyStart, final ByteBuffer keyEnd, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, null, null, (PersistitTransaction) txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, KeySelector selector, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, selector, null, (PersistitTransaction) txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException {
        return getSlice(keyStart, keyEnd, null, limit, (PersistitTransaction) txh);
    }

    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value, final StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                toKey(exchange, key);
                setValue(exchange, value);
            }
        };
        final PersistitTransaction tx = (PersistitTransaction) txh;
        final Exchange exchange = tx.getExchange(name);
        try {
            j.setExchange(exchange);
            tx.run(j);
        } finally {
            tx.releaseExchange(exchange);
        }
    }

    @Override
    public void delete(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                toKey(exchange, key);
                exchange.remove();
            }
        };
        final PersistitTransaction tx = (PersistitTransaction) txh;
        final Exchange exchange = tx.getExchange(name);
        try {
            j.setExchange(exchange);
            tx.run(j);
        } finally {
            tx.releaseExchange(exchange);
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
