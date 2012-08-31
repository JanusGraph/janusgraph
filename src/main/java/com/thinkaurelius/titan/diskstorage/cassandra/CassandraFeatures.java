package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.StorageFeatures;
import com.thinkaurelius.titan.diskstorage.util.StorageFeaturesImplementation;
import org.apache.commons.configuration.Configuration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class CassandraFeatures {

    private static final StorageFeatures INSTANCE = new StorageFeaturesImplementation(false,false);

    public static final StorageFeatures of(Configuration storageConfig) {
        return INSTANCE;
    }

}
