package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageFeatures;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StorageFeaturesImplementation implements StorageFeatures {

    private boolean supportsScan;
    private boolean isTransactional;
    
    public StorageFeaturesImplementation(boolean supportsScan, boolean isTransactional) {
        this.supportsScan=supportsScan;
        this.isTransactional=isTransactional;
    }
    
    public StorageFeaturesImplementation(Map<String,Boolean> settings) {
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

    @Override
    public boolean supportsScan() {
        return supportsScan;
    }

    @Override
    public boolean isTransactional() {
        return isTransactional;
    }
}
