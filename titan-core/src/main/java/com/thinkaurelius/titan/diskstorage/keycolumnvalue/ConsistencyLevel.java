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
    KEY_CONSISTENT,

    /**
     * Consistency level which ensures that operations on a {@link KeyColumnValueStore} are key-consistent with
     * respect to a local cluster where multiple local clusters form the entire (global) cluster.
     * In other words, {@link #KEY_CONSISTENT} ensures key consistency across the entire global cluster whereas this
     * is restricted to the local cluster.
     */
    LOCAL_KEY_CONSISTENT;


    public boolean isKeyConsistent() {
        switch (this) {
            case KEY_CONSISTENT:
            case LOCAL_KEY_CONSISTENT:
                return true;
            case DEFAULT:
                return false;
            default: throw new AssertionError(this.toString());
        }
    }

}
