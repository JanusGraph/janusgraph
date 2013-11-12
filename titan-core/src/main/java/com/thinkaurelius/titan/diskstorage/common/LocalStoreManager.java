package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.DirectoryUtil;
import org.apache.commons.configuration.Configuration;

import java.io.File;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;

/**
 * Abstract Store Manager used as the basis for local StoreManager implementations.
 * Simplifies common configuration management.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class LocalStoreManager extends AbstractStoreManager {

    protected final File directory;

    public LocalStoreManager(Configuration storageConfig) throws StorageException {
        super(storageConfig);
        String storageDir = storageConfig.getString(STORAGE_DIRECTORY_KEY);
        if (null == storageDir) {
            directory = null;
        } else { 
            directory = DirectoryUtil.getOrCreateDataDirectory(storageDir);
        }
    }
}
