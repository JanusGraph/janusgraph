package com.thinkaurelius.titan.diskstorage.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class PersistitKeyValueTest extends KeyValueStoreTest {

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws StorageException {
        Configuration config = PersistitStorageSetup.getPersistitGraphConfig();
        return new PersistitStoreManager(config.subset(STORAGE_NAMESPACE));
    }
}
