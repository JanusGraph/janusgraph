package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY;
import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY;
import static org.junit.Assert.*;

public class CassandraTransactionTest {

    /* testRead/WriteConsistencyLevel have unnecessary code duplication
     * that could be avoided by creating a common helper method that takes
     * a ConfigOption parameter and a function that converts a
     * CassandraTransaction to a consistency level by calling either
     * ct.getReadConsistencyLevel() or .getWriteConsistencyLevel(),
     * but it doesn't seem worth the complexity.
     */

    @Test
    public void testWriteConsistencyLevel() {
        int levelsChecked = 0;

        // Test whether CassandraTransaction honors the write consistency level option
        for (CLevel writeLevel : CLevel.values()) {
            StandardBaseTransactionConfig.Builder b = new StandardBaseTransactionConfig.Builder();
            ModifiableConfiguration mc = GraphDatabaseConfiguration.buildConfiguration();
            mc.set(CASSANDRA_WRITE_CONSISTENCY, writeLevel.name());
            b.customOptions(mc);
            b.timestampProvider(Timestamps.MICRO);
            CassandraTransaction ct = new CassandraTransaction(b.build());
            assertEquals(writeLevel, ct.getWriteConsistencyLevel());
            levelsChecked++;
        }

        // Sanity check: if CLevel.values was empty, something is wrong with the test
        Preconditions.checkState(0 < levelsChecked);
    }

    @Test
    public void testReadConsistencyLevel() {
        int levelsChecked = 0;

        // Test whether CassandraTransaction honors the write consistency level option
        for (CLevel writeLevel : CLevel.values()) {
            StandardBaseTransactionConfig.Builder b = new StandardBaseTransactionConfig.Builder();
            ModifiableConfiguration mc = GraphDatabaseConfiguration.buildConfiguration();
            mc.set(CASSANDRA_READ_CONSISTENCY, writeLevel.name());
            b.timestampProvider(Timestamps.MICRO);
            b.customOptions(mc);
            CassandraTransaction ct = new CassandraTransaction(b.build());
            assertEquals(writeLevel, ct.getReadConsistencyLevel());
            levelsChecked++;
        }

        // Sanity check: if CLevel.values was empty, something is wrong with the test
        Preconditions.checkState(0 < levelsChecked);
    }

    @Test
    public void testTimestampProvider() {
        BaseTransactionConfig txcfg = StandardBaseTransactionConfig.of(Timestamps.NANO);
        CassandraTransaction ct = new CassandraTransaction(txcfg);
        assertEquals(Timestamps.NANO, ct.getConfiguration().getTimestampProvider());

        txcfg = StandardBaseTransactionConfig.of(Timestamps.MICRO);
        ct = new CassandraTransaction(txcfg);
        assertEquals(Timestamps.MICRO, ct.getConfiguration().getTimestampProvider());

        txcfg = StandardBaseTransactionConfig.of(Timestamps.MILLI);
        ct = new CassandraTransaction(txcfg);
        assertEquals(Timestamps.MILLI, ct.getConfiguration().getTimestampProvider());
    }
}
