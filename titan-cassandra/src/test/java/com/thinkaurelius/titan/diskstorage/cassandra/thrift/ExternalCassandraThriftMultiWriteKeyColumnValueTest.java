package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraThriftMultiWriteKeyColumnValueTest extends MultiWriteKeyColumnValueStoreTest {


    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraThriftStoreManager(CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName()));
    }


    @BeforeClass
    public static void startCassandra() {
        ch.startCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
        ch.stopCassandra();
    }

}
