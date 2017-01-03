package org.janusgraph.blueprints;

import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.GraphProvider;

/**
 * Created by bryn on 06/05/15.
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class InMemoryGraphComputerProvider extends AbstractJanusGraphComputerProvider {

    @Override
    public ModifiableConfiguration getJanusConfiguration(String graphName, Class<?> test, String testMethodName) {
        ModifiableConfiguration config = StorageSetup.getInMemoryConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL,false);
        return config;
    }
}
