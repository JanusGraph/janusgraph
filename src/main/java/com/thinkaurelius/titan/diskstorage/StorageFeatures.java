package com.thinkaurelius.titan.diskstorage;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface StorageFeatures {

    /**
     * Whether this storage backend supports global key scans through the interface {@link ScanKeyColumnValueStore}
     *
     * @return
     */
    public boolean supportsScan();

    /**
     * Whether this storage backend is transactional.
     *
     * @return
     */
    public boolean isTransactional();

}
