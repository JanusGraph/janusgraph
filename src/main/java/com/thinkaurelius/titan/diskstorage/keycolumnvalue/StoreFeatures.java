package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Specifies the features that a given store supports
 *
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StoreFeatures {

    private boolean supportsScan;
    private boolean isTransactional;
    private boolean supportsConsistentKeyOperations;
    private boolean supportsLocking;
    private boolean supportsBatchMutation;
    
    public StoreFeatures(boolean supportsScan, boolean isTransactional, boolean supportsConsistentKeyOperations, boolean supportsLocking, boolean supportsBatchMutation) {
        this.supportsScan=supportsScan;
        this.isTransactional=isTransactional;
        this.supportsConsistentKeyOperations=supportsConsistentKeyOperations;
        this.supportsLocking=supportsLocking;
        this.supportsBatchMutation = supportsBatchMutation;
    }
    
    public StoreFeatures(Map<String, Boolean> settings) {
        for (Field f : getClass().getDeclaredFields()) {
            Preconditions.checkArgument(settings.containsKey(f.getName()),"Settings do not contain: " + f.getName());
            Boolean value = settings.get(f.getName());
            Preconditions.checkNotNull(value,"Value may not be null for setting: "+f.getName());
            try {
                f.setBoolean(this,value);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Could not change setting: " + f.getName(),e);
            }
        }
    }

    /**
     * Whether this storage backend supports global key scans via (
     *
     * @return
     */
    public boolean supportsScan() {
        return supportsScan;
    }

    /**
     * Whether this storage backend is transactional.
     *
     * @return
     */
    public boolean isTransactional() {
        return isTransactional;
    }

    /**
     * Whether this store supports consistent atomic operations on keys.
     *
     * @return
     */
    public boolean supportsConsistentKeyOperations() {
        return supportsConsistentKeyOperations;
    }

    /**
     * Whether this store supports locking via {@link KeyColumnValueStore#acquireLock(java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer, StoreTransactionHandle)}
     *
     * @return
     */
    public boolean supportsLocking() {
        return supportsLocking;
    }

    public boolean supportsBatchMutation() {
        return supportsBatchMutation;
    }
}
