package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.CassandraGraphTest;

public class EmbeddedGraphTest extends CassandraGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }
}
