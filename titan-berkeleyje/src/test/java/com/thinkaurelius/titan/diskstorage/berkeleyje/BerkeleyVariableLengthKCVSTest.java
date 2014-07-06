package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;


public class BerkeleyVariableLengthKCVSTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
