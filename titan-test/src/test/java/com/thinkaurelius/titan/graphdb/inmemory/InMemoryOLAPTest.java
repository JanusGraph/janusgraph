package com.thinkaurelius.titan.graphdb.inmemory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.olap.FulgoraOLAPTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryOLAPTest extends FulgoraOLAPTest {

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

}
