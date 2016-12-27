package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryKeyColumnValueStoreTest extends KeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new InMemoryStoreManager();
    }

    @Override
    public void clopen() {
        //Do nothing
    }

}
