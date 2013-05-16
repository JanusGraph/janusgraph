package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;


import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * Determines which keys match a particular retrieval request against a {@link OrderedKeyValueStore}.
 *
 * @see OrderedKeyValueStore#getSlice(com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, KeySelector, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeySelector {

    /**
     * KeySelector that returns all keys as matching
     */
    public static final KeySelector SelectAll = new KeySelector() {

        @Override
        public boolean include(StaticBuffer key) {
            return true;
        }

        @Override
        public boolean reachedLimit() {
            return false;
        }

    };

    /**
     * Whether key should be included in the result set.
     * @param key
     * @return
     */
    public boolean include(StaticBuffer key);

    /**
     * Whether the retrieval limit has been reached.
     * @return
     */
    public boolean reachedLimit();

}
