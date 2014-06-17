package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanEventualGraphTest;
import com.thinkaurelius.titan.graphdb.TitanPartitionGraphTest;
import org.junit.BeforeClass;

public class EmbeddedPartitionGraphTest extends TitanPartitionGraphTest {

    @BeforeClass
    public static void startEmbeddedCassandra() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
