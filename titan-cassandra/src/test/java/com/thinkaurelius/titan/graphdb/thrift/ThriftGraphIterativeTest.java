package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.graphdb.TitanGraphIterativeBenchmark;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.BeforeClass;

public class ThriftGraphIterativeTest extends TitanGraphIterativeBenchmark {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraThriftStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,getConfiguration(), BasicConfiguration.Restriction.NONE));
    }


    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }
}
