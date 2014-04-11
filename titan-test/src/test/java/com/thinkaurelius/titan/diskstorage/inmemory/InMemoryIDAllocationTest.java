package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.IDAllocationTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryIDAllocationTest extends IDAllocationTest {

    public InMemoryIDAllocationTest(WriteConfiguration baseConfig) {
        super(baseConfig);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager(int id) throws StorageException {
        return new InMemoryStoreManager();
    }

    @Override
    public void testMultiIDAcquisition() {
        //DO nothing - TODO: should this work??
    }


}
