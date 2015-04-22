package com.thinkaurelius.titan.diskstorage;

/**
 * Store-specific (Columnfamily-specific) options passed between
 * Titan core and its underlying KeyColumnValueStore implementation.
 * This is part of Titan's internals and is not user-facing in
 * ordinary operation.
 */
public enum StoreMetaData {

    /**
     * Time-to-live for all data written to the store.  Values associated
     * with this enum will be expressed in seconds.  The TTL is only required
     * to be honored when the associated store is opened for the first time.
     * Subsequent reopenings of an existing store need not check for or
     * modify the existing TTL (though implementations are free to do so).
     */
    TTL
}
