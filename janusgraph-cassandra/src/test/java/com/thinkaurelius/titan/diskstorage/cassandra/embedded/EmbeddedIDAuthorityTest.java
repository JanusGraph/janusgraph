package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.IDAuthorityTest;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class EmbeddedIDAuthorityTest extends IDAuthorityTest {

    public EmbeddedIDAuthorityTest(WriteConfiguration baseConfig) {
        super(baseConfig);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CassandraEmbeddedStoreManager(CassandraStorageSetup.getEmbeddedConfiguration(getClass().getSimpleName()));
    }
}
