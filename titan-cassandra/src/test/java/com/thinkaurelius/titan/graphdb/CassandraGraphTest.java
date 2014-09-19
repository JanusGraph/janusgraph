package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.*;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class CassandraGraphTest extends TitanGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

    @Test
    public void testHasTTL() throws Exception {
        assertTrue(features.hasCellTTL());
    }

    @Test
    public void testGraphConfigUsedByThreadBoundTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "LOCAL_QUORUM");

        graph = (StandardTitanGraph) TitanFactory.open(wc);

        StandardTitanTx tx = (StandardTitanTx)graph.getCurrentThreadTx();
        assertEquals("ALL",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("LOCAL_QUORUM",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
    }

    @Test
    public void testGraphConfigUsedByTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "TWO");
        wc.set(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "THREE");

        graph = (StandardTitanGraph) TitanFactory.open(wc);

        StandardTitanTx tx = (StandardTitanTx)graph.newTransaction();
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("THREE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testCustomConfigUsedByTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "ALL");

        graph = (StandardTitanGraph) TitanFactory.open(wc);

        StandardTitanTx tx = (StandardTitanTx)graph.buildTransaction()
                .setCustomOption(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ONE")
                .setCustomOption(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "TWO").start();

        assertEquals("ONE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
        tx.rollback();
    }
}
