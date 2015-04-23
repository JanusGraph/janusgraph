package com.thinkaurelius.titan.graphdb.inmemory;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanPartitionGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryPartitionGraphTest extends TitanPartitionGraphTest {

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
