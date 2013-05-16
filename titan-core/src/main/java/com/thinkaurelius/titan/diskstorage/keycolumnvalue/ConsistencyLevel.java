package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

/**
 * Consistency Levels for transactions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ConsistencyLevel {

    /**
     * The default consistency level afforded by the underlying storage backend
     */
    DEFAULT,

    /**
     * Consistency level which ensures that operations on a {@link KeyColumnValueStore} are key-consistent.
     */
    KEY_CONSISTENT;

}
