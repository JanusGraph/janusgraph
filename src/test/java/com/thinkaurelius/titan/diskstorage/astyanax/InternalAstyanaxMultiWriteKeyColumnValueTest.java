package com.thinkaurelius.titan.diskstorage.astyanax;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStorageManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;

public class InternalAstyanaxMultiWriteKeyColumnValueTest extends MultiWriteKeyColumnValueStoreTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}
	
    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new AstyanaxStorageManager(StorageSetup.getCassandraStorageConfiguration());
    }
}
