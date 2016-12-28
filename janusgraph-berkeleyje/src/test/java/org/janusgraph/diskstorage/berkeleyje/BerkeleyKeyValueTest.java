package org.janusgraph.diskstorage.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;


public class BerkeleyKeyValueTest extends KeyValueStoreTest {

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
    }


}
