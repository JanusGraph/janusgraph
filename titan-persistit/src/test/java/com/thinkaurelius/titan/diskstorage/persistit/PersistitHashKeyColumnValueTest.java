package com.thinkaurelius.titan.diskstorage.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

public class PersistitHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = PersistitStorageSetup.getPersistitGraphConfig();
        PersistitStoreManager sm = new PersistitStoreManager(config.subset(STORAGE_NAMESPACE));

        // The same as BerkeleyJE in hash prefixed mode, Persistit doesn't support ordered key iteration
        sm.features.supportsOrderedScan = false;
        sm.features.supportsUnorderedScan = false;

        return new OrderedKeyValueStoreManagerAdapter(sm);
    }
    
    @Test
    @Override
    public void testGetKeysWithKeyRange() {
        // Requires ordered keys, but we're using hash prefix
    }
    
    @Test
    @Override
    public void testOrderedGetKeysRespectsKeyLimit() {
        // Requires ordered keys, but we are using hash prefix
    }
}
