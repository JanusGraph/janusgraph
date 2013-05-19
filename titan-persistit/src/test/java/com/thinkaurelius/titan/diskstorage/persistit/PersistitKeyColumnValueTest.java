package com.thinkaurelius.titan.diskstorage.persistit;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import org.apache.commons.configuration.Configuration;

public class PersistitKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = PersistitStorageSetup.getPersistitGraphConfig();
        PersistitStoreManager sm = new PersistitStoreManager(config.subset(STORAGE_NAMESPACE));
        KeyValueStoreManagerAdapter smadapter = new KeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
        return smadapter;
    }

}
