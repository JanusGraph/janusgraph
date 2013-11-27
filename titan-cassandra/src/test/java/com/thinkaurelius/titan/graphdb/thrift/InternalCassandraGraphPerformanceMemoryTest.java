package com.thinkaurelius.titan.graphdb.thrift;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class InternalCassandraGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    public InternalCassandraGraphPerformanceMemoryTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphPerformanceMemoryTest.class.getSimpleName()));
    }
    
    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }
}
