package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.*;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;

import java.awt.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

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
    private static abstract class PersistitJob implements TransactionRunnable {
        Object result = null;

        /**
         * Returns the result of the job
         * @return
         */
        public Object getResult() {
            return result;
        }
    }

    private final String name;
    private final PersistitStoreManager storeManager;
    private final Persistit persistit;
    private final Exchange exchange;

    public PersistitKeyValueStore(String n, PersistitStoreManager mgr, Persistit db) throws StorageException {
        name = n;
        storeManager = mgr;
        persistit = db;

        try {
            exchange = persistit.getExchange("titan", name, true);
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
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
            exchange.removeAll();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    private static class KeysIterator implements RecordIterator<ByteBuffer> {

        final PersistitTransaction transaction;
        final Exchange exchange;
        final Persistit persistit;
        Object nextKey = null;

        private PersistitJob getNextKeyJob;

        public KeysIterator(PersistitTransaction tx, Exchange ex, Persistit ps) throws StorageException {
            transaction = tx;

            // clone the given exchange so we can iterate over it independent of
            // any other operations on this KeyValueStore instance
            exchange = new Exchange(ex);
            persistit = ps;

            getNextKeyJob = new PersistitJob() {
                @Override
                public void runTransaction() throws PersistitException, RollbackException {
                    if (exchange.hasNext()) {
                        exchange.next();
                        //@todo: check that bytes are, in fact, always deserialized as strings
                        result = ((String)exchange.getKey().decode()).getBytes();
                    } else {
                        result = null;
                    }
                }
            };

            begin();
            getNextKey();
        }

        private void getNextKey() throws StorageException {
            transaction.run(getNextKeyJob);
            nextKey = getNextKeyJob.getResult();
        }

        private void begin() throws StorageException {
            transaction.run(new PersistitJob() {
                @Override
                public void runTransaction() throws PersistitException, RollbackException {
                    exchange.getKey().to(Key.BEFORE);
                }
            });
        }

        @Override
        public boolean hasNext() {
            return nextKey != null;
        }

        @Override
        public ByteBuffer next() throws StorageException {
            if (nextKey == null) throw new NoSuchElementException();
            ByteBuffer returnKey = ByteBuffer.wrap((byte[])nextKey);
            getNextKey();
            return returnKey;
        }

        @Override
        public void close() {
            //don't do a damn thing
        }
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction tx) throws StorageException {
        return new KeysIterator((PersistitTransaction)tx, exchange, persistit);
    }

    @Override
    public ByteBuffer get(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                byte[] k = key.array();
                Key ek = exchange.getKey();
                ek.clear();
                ek.appendByteArray(k, 0, k.length);

                exchange.fetch();
                result = ByteBuffer.wrap(exchange.getValue().getByteArray());
            }
        };
        ((PersistitTransaction) txh).run(j);
        return (ByteBuffer) j.getResult();
    }

    @Override
    public boolean containsKey(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                byte[] k = key.array();
                Key ek = exchange.getKey();
                ek.clear();
                ek.appendByteArray(k, 0, k.length);
                result = exchange.isValueDefined();
            }
        };
        ((PersistitTransaction) txh).run(j);
        return (Boolean) j.getResult();
    }

    /**
     * Runs all getSlice queries
     * @todo: this
     * @todo: question/assumption: is keyStart guaranteed to evaluate to <= keyEnd?
     *
     * @param keyStart
     * @param keyEnd
     * @param selector
     * @param limit
     * @param txh
     * @return
     * @throws StorageException
     */
    private List<KeyValueEntry> getSlice(final ByteBuffer keyStart, final ByteBuffer keyEnd, final KeySelector selector, final Integer limit, PersistitTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                ArrayList<KeyValueEntry> results = new ArrayList<KeyValueEntry>();

                byte[] start = keyStart.array();
                byte[] end = keyEnd.array();

                KeyFilter.Term[] terms = {KeyFilter.rangeTerm(start, end)};
                KeyFilter keyFilter = new KeyFilter(terms);

                exchange.to(start);

                int i = 0;
                while (keyFilter.selected(exchange.getKey())) {
                    if (limit != null && i >= limit) break;
                    if (selector != null && selector.reachedLimit()) break;

                    ByteBuffer k = ByteBuffer.wrap(exchange.getKey().decodeByteArray());
                    ByteBuffer v = ByteBuffer.wrap(exchange.getValue().getByteArray());

                    if (selector == null || selector.include(k)){
                        KeyValueEntry kv = new KeyValueEntry(k, v);
                        results.add(kv);
                        i++;
                    }
                }
                result = results;
            }
        };

        txh.run(j);
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
                byte[] k = key.array();
                byte[] v = value.array();
                Key ek = exchange.getKey();
                ek.clear();
                ek.appendByteArray(k, 0, k.length);

                Value ev = exchange.getValue();
                ev.clear();
                ev.putByteArray(k, 0, k.length);

                exchange.store();
            }
        };
        ((PersistitTransaction) txh).run(j);
    }

    @Override
    public void delete(final ByteBuffer key, StoreTransaction txh) throws StorageException {
        PersistitJob j = new PersistitJob() {
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                byte[] k = key.array();
                Key ek = exchange.getKey();
                ek.clear();
                ek.appendByteArray(k, 0, k.length);
                exchange.remove();
            }
        };
        ((PersistitTransaction) txh).run(j);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        //@todo: what is this supposed to do? BerkelyDB doesn't really implement this
    }

    @Override
    public void close() throws StorageException {
        //@todo: do we need to do anything here?? I think that as long as the persistit db is closed, this should be ok
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }
}
