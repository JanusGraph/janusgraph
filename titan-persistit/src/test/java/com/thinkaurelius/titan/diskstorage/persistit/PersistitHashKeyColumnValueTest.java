package com.thinkaurelius.titan.diskstorage.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.apache.commons.configuration.Configuration;

public class PersistitHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = PersistitStorageSetup.getPersistitGraphConfig();
        PersistitStoreManager sm = new PersistitStoreManager(config.subset(STORAGE_NAMESPACE));

        // The same as BerkeleyJE in hash prefixed mode, Persistit doesn't support ordered key iteration
        sm.features.supportsScan = false;

        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Override
    public void testGetKeysWithSliceQuery() throws Exception {
        // This is not going to work as getKeys(SliceQuery) is not supported by ordered store and
        // as this store is going to be prefixed it also means that we can't use getKeys(KeyRangeQuery)
    }
}
