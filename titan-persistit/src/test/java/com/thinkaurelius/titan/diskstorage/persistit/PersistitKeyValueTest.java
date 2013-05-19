package com.thinkaurelius.titan.diskstorage.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import org.apache.commons.configuration.Configuration;

public class PersistitKeyValueTest extends KeyValueStoreTest {

    @Override
    public KeyValueStoreManager openStorageManager() throws StorageException {
        Configuration config = PersistitStorageSetup.getPersistitGraphConfig();
        return new PersistitStoreManager(config.subset(STORAGE_NAMESPACE));
    }
}
