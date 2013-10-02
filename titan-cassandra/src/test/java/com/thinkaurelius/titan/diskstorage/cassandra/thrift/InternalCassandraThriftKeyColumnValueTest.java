package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;

public class InternalCassandraThriftKeyColumnValueTest extends AbstractCassandraKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public Configuration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration c) throws StorageException {
        return new CassandraThriftStoreManager(c);
    }
}
