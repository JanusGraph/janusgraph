package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.TitanNonTransactionalGraphMetricsTest;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

public class InternalCassandraGraphMetricsTest extends TitanNonTransactionalGraphMetricsTest {

    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public Configuration getConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphMetricsTest.class.getSimpleName());
    }
}
