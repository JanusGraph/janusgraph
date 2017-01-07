package org.janusgraph.graphdb.inmemory;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryPartitionGraphTest extends JanusGraphPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.IDS_FLUSH,false);
        return config.getConfiguration();
    }

    @Override
    public void clopen(Object... settings) {
        newTx();
    }

    @Override
    public void testPartitionSpreadFlushBatch() {
    }

    @Override
    public void testPartitionSpreadFlushNoBatch() {
    }

    @Override
    public void testKeybasedGraphPartitioning() {}

}
