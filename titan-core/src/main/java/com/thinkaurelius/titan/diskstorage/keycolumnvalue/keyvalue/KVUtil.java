package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility methods for interacting with {@link KeyValueStore}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KVUtil {
    public static final RecordIterator<KeyValueEntry> EMPTY_ITERATOR = new RecordIterator<KeyValueEntry>() {
        @Override
        public boolean hasNext() throws StorageException {
            return false;
        }

        @Override
        public KeyValueEntry next() throws StorageException {
            throw new NoSuchElementException();
        }

        @Override
        public void close() throws StorageException {
        }
    };

    public static List<KeyValueEntry> getSlice(OrderedKeyValueStore store, StaticBuffer keyStart, StaticBuffer keyEnd, StoreTransaction txh) throws StorageException {
        return convert(store.getSlice(keyStart, keyEnd, KeySelector.SelectAll, txh));
    }

    public static List<KeyValueEntry> getSlice(OrderedKeyValueStore store, StaticBuffer keyStart, StaticBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException {
        return convert(store.getSlice(keyStart, keyEnd, new LimitedSelector(limit), txh));
    }

    public static List<KeyValueEntry> convert(RecordIterator<KeyValueEntry> iter) throws StorageException {
        List<KeyValueEntry> entries = new ArrayList<KeyValueEntry>();
        while (iter.hasNext()) {
            entries.add(iter.next());
        }
        iter.close();
        return entries;
    }

    public static class RangeKeySelector implements KeySelector {

        private final StaticBuffer lower; //inclusive
        private final StaticBuffer upper; //exclusive

        public RangeKeySelector(StaticBuffer lower, StaticBuffer upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean include(StaticBuffer key) {
            return lower.compareTo(key) <= 0 && upper.compareTo(key) > 0;
        }

        @Override
        public boolean reachedLimit() {
            return false;
        }
    }
}
