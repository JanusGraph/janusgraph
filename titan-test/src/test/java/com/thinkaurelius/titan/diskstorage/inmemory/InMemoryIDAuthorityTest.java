package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.IDAuthorityTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryIDAuthorityTest extends IDAuthorityTest {

    /**
     * The IDAllocationTest assumes that every StoreManager returned by
     * {@link #openStorageManager()} can see one another's reads and writes. In
     * the HBase and Cassandra tests, we can open a new StoreManager in every
     * call to {@code openStorageManager} and they will all satisfy this
     * constraint, since every manager opens with the same config and talks to
     * the same service. It's not really necessary to have separate managers,
     * but it's nice for getting an extra bit of test coverage. However,
     * separate in-memory managers wouldn't be able to see one another's
     * reads/writes, so we just open a single manager and store it here.
     */
    private final InMemoryStoreManager sharedManager;

    public InMemoryIDAuthorityTest(WriteConfiguration baseConfig) {
        super(baseConfig);
        sharedManager = new InMemoryStoreManager();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return sharedManager;
    }
}
