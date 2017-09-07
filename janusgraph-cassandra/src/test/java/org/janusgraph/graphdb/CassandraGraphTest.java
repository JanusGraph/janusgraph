// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;
import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class CassandraGraphTest extends JanusGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
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

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx)graph.getCurrentThreadTx();
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

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx)graph.newTransaction();
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

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx)graph.buildTransaction()
                .customOption(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ONE")
                .customOption(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "TWO").start();

        assertEquals("ONE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
        tx.rollback();
    }
    
    @Test
    public void testTitanGraphBackwardCompatibility() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_KEYSPACE), "titan");
        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "x.x.x");
        
        assertNull(wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION), 
                            GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION.getDatatype()));
        
        assertFalse(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
                            wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), 
                                        GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())
                ));

        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "1.0.0");
        assertTrue(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
                            wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), 
                                        GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())
                ));
        
        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.IDS_STORE_NAME), JanusGraphConstants.TITAN_ID_STORE_NAME);
        assertTrue(JanusGraphConstants.TITAN_ID_STORE_NAME.equals(
                            wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.IDS_STORE_NAME), 
                                        GraphDatabaseConfiguration.IDS_STORE_NAME.getDatatype())
                ));
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
    }
}
