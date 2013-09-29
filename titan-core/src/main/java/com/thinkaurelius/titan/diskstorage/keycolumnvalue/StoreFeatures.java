package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;

/**
 * Specifies the features that a given store supports
 * <p/>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StoreFeatures {

    public Boolean supportsUnorderedScan;
    public Boolean supportsOrderedScan;
    public Boolean supportsBatchMutation;

    public Boolean supportsTransactions;
    public Boolean supportsConsistentKeyOperations;
    public Boolean supportsLocking;

    public Boolean isKeyOrdered;
    public Boolean isDistributed;
    public Boolean hasLocalKeyPartition;

    private void verify() {
        for (Field f : getClass().getDeclaredFields()) {
            try {
                Preconditions.checkArgument(f.get(this) != null, "Setting has not been specified: " + f.getName());
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Could not inspect setting: " + f.getName(), e);
            }
        }
    }

    @Override
    public StoreFeatures clone() {
        StoreFeatures newfeatures = new StoreFeatures();
        for (Field f : getClass().getDeclaredFields()) {
            try {
                f.set(newfeatures, f.get(this));
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Could not copy setting: " + f.getName(), e);
            }
        }
        return newfeatures;
    }

    /**
     * Whether this storage backends supports scanning of any kind
     *
     * @return
     */
    public boolean supportsScan() {
        return supportsOrderedScan() || supportsUnorderedScan();
    }


    /**
     * Whether this storage backend supports global key scans via {@link KeyColumnValueStore#getKeys(SliceQuery, StoreTransaction)}
     *
     * @return
     */
    public boolean supportsUnorderedScan() {
        verify();
        return supportsUnorderedScan;
    }

    /**
     * Whether this storage backend supports global key scans via {@link KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction)}
     *
     * @return
     */
    public boolean supportsOrderedScan() {
        verify();
        return supportsOrderedScan;
    }

    /**
     * Whether this storage backend is transactional.
     *
     * @return
     */
    public boolean supportsTransactions() {
        verify();
        return supportsTransactions;
    }

    /**
     * Whether this store supports consistent atomic operations on keys.
     *
     * @return
     */
    public boolean supportsConsistentKeyOperations() {
        verify();
        return supportsConsistentKeyOperations;
    }

    /**
     * Whether this store supports locking via {@link KeyColumnValueStore#acquireLock(com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, StoreTransaction)}
     *
     * @return
     */
    public boolean supportsLocking() {
        verify();
        return supportsLocking;
    }

    /**
     * Whether this storage backend supports batch mutations via {@link KeyColumnValueStoreManager#mutateMany(java.util.Map, StoreTransaction)}.
     *
     * @return
     */
    public boolean supportsBatchMutation() {
        verify();
        return supportsBatchMutation;
    }

    public boolean isKeyOrdered() {
        verify();
        return isKeyOrdered;
    }

    public boolean isDistributed() {
        verify();
        return isDistributed;
    }

    public boolean hasLocalKeyPartition() {
        verify();
        return hasLocalKeyPartition;
    }
}
