package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.IDAllocationTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class InternalCassandraEmbeddedIDAllocationTest extends IDAllocationTest {

    public InternalCassandraEmbeddedIDAllocationTest(WriteConfiguration baseConfig) {
        super(baseConfig);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        return new CassandraEmbeddedStoreManager(CassandraStorageSetup.getEmbeddedConfiguration(getClass().getSimpleName()));
    }
}
