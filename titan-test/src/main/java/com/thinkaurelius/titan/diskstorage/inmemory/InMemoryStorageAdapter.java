package com.thinkaurelius.titan.diskstorage.inmemory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

import org.apache.commons.configuration.Configuration;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryStorageAdapter implements KeyColumnValueStoreManager {

    public InMemoryStorageAdapter(Configuration config) {

    }

    @Override
    public KeyColumnValueStore openDatabase(final String name) throws StorageException {
        return new KeyColumnValueStore() {
            @Override
            public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
                return false;
            }

            @Override
            public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
                return ImmutableList.of();
            }

            public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
                return ImmutableList.of();
            }

            @Override
            public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
                //Do nothing
            }

            @Override
            public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
                //Do nothing
            }

            @Override
            public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws StorageException {
                return new EmptyRowIterator();
            }

            @Override
            public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
                return new EmptyRowIterator();
            }

            @Override
            public StaticBuffer[] getLocalKeyPartition() throws StorageException {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public void close() throws StorageException {
                //Do nothing
            }
        };
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        //Do nothing
    }

    @Override
    public StoreTransaction beginTransaction(final StoreTxConfig config) throws StorageException {
        return new StoreTransaction() {
            @Override
            public StoreTxConfig getConfiguration() {
                return config;
            }

            @Override
            public void commit() throws StorageException {
                //Do nothing
            }

            @Override
            public void rollback() throws StorageException {
                //Do nothing
            }

            @Override
            public void flush() throws StorageException {
                //Do nothing
            }
        };
    }

    @Override
    public void close() throws StorageException {
        //Do nothing
    }

    @Override
    public void clearStorage() throws StorageException {
        //Do nothing
    }

    @Override
    public StoreFeatures getFeatures() {
        StoreFeatures f = new StoreFeatures();
        f.supportsUnorderedScan = true;
        f.supportsOrderedScan = true;
        f.supportsBatchMutation = true;

        f.supportsTransactions = true;
        f.supportsConsistentKeyOperations = false;
        f.supportsLocking = true;

        f.isKeyOrdered = false;
        f.isDistributed = false;
        f.hasLocalKeyPartition = false;
        return f;
    }

    private final Map<String, String> config = Maps.newHashMap();

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return config.get(key);
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        config.put(key, value);
    }

    private static class EmptyRowIterator implements KeyIterator {
        @Override
        public RecordIterator<Entry> getEntries() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public StaticBuffer next() {
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            // Do nothing
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can't remove element from empty iterator");
        }
    }

    @Override
    public String getName() {
        return toString();
    }
}
