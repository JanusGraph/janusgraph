package com.thinkaurelius.titan.pkgtest;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.thinkaurelius.titan.testcategory.UnorderedKeyStoreTests;

public class FaunusCassandraIT extends AbstractTitanAssemblyIT {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGraphOfTheGodsWithBOP() throws Exception {
        unzipAndRunExpect("faunus-cassandra.expect.vm", ImmutableMap.of("cassandraPartitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner"));
    }

    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testGraphOfTheGodsWithMurmur() throws Exception {
        unzipAndRunExpect("faunus-cassandra.expect.vm", ImmutableMap.of("cassandraPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner"));
    }
}
