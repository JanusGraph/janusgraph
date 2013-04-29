package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import java.nio.ByteBuffer;

/**
 * Determines which keys match a particular retrieval request against a {@link OrderedKeyValueStore}.
 *
 * @see OrderedKeyValueStore#getSlice(java.nio.ByteBuffer, java.nio.ByteBuffer, KeySelector, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeySelector {

    /**
     * KeySelector that returns all keys as matching
     */
    public static final KeySelector SelectAll = new KeySelector() {

        @Override
        public boolean include(ByteBuffer key) {
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
    public boolean include(ByteBuffer key);

    /**
     * Whether the retrieval limit has been reached.
     * @return
     */
    public boolean reachedLimit();

}
