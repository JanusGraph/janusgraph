package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStorePerformance;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryKCVSP extends KeyColumnValueStorePerformance {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new InMemoryStoreManager();
    }

    @Override
    public void clopen() {
        //Do nothing
    }

}
