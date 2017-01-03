package org.janusgraph.blueprints;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyJETx;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.GraphProvider;

import java.time.Duration;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class BerkeleyGraphComputerProvider extends AbstractJanusGraphComputerProvider {

    @Override
    public ModifiableConfiguration getJanusConfiguration(String graphName, Class<?> test, String testMethodName) {
        ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration(StorageSetup.getHomeDir(graphName));
        config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(20));
        config.set(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL,false);
        return config;
    }

    @Override
    public Set<Class> getImplementations() {
        final Set<Class> implementations = super.getImplementations();
        implementations.add(BerkeleyJETx.class);
        return implementations;
    }

}
