package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.CassandraStorageSetup;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

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
}
