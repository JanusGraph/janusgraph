package com.thinkaurelius.titan.tinkerpop.rexster;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.testcategory.ByteOrderedPartitionerTests;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({ByteOrderedPartitionerTests.class})
public class RexsterServerClientCassandraTest extends RexsterServerClientTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }
    
    public RexsterServerClientCassandraTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(RexsterServerClientCassandraTest.class.getSimpleName()));
    }

}
