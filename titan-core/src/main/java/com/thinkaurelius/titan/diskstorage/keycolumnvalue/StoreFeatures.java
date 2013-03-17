package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;

/**
 * Specifies the features that a given store supports
 * <p/>
 * (c) Matthias Broecheler (me@matthiasb.com)
 */
public class StoreFeatures {

    public Boolean supportsScan;
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
     * Whether this storage backend supports global key scans via (
     *
     * @return
     */
    public boolean supportsScan() {
        verify();
        return supportsScan;
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
     * Whether this store supports locking via {@link KeyColumnValueStore#acquireLock(java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer, StoreTransaction)}
     *
     * @return
     */
    public boolean supportsLocking() {
        verify();
        return supportsLocking;
    }

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
