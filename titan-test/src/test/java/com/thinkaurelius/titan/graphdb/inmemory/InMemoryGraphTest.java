package com.thinkaurelius.titan.graphdb.inmemory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryGraphTest extends TitanGraphTest {

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        return config.getConfiguration();
    }

    @Override
    public void clopen(Object... settings) {
        Preconditions.checkArgument(settings==null || settings.length==0);
        newTx();
    }

    @Test
    public void testLocalGraphConfiguration() {}

    @Test
    public void testMaskableGraphConfig() {}

    @Test
    public void testGlobalGraphConfig() {}

    @Test
    public void testGlobalOfflineGraphConfig() {}

    @Test
    public void testFixedGraphConfig() {}

    @Override
    public void testManagedOptionMasking() {}

    @Override
    public void testTransactionConfiguration() {}

    @Override
    public void testDataTypes() {}

    @Override
    public void testForceIndexUsage() {}

    @Override
    public void testAutomaticTypeCreation() {}

    @Override
    public void simpleLogTest() {}

    @Override
    public void simpleLogTestWithFailure() {}

    @Override
    public void testIndexUpdatesWithoutReindex() {}

    @Override
    public void testIndexUpdateSyncWithMultipleInstances() {}

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

}
