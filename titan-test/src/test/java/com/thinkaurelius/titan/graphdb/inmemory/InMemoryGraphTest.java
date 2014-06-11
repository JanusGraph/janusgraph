package com.thinkaurelius.titan.graphdb.inmemory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.blueprints.TitanSpecificBlueprintsTestSuite;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.util.ElementHelper;
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

    @Override
    public void testConfiguration() {}

    @Override
    public void testDataTypes() {}

    @Override
    public void testForceIndexUsage() {}

    @Override
    public void testAutomaticTypeCreation() {}

    @Override
    public void simpleLogTest() {}

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

}
