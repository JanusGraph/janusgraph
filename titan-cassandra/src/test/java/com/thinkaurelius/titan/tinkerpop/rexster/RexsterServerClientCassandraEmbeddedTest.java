package com.thinkaurelius.titan.tinkerpop.rexster;

import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.testcategory.ByteOrderedPartitionerTests;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({ByteOrderedPartitionerTests.class})
public class RexsterServerClientCassandraEmbeddedTest extends RexsterServerClientTest {
    public RexsterServerClientCassandraEmbeddedTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(RexsterServerClientCassandraEmbeddedTest.class.getSimpleName()));
    }

}
