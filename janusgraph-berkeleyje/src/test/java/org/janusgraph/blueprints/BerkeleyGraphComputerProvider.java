package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJETx;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.GraphProvider;

import java.time.Duration;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class BerkeleyGraphComputerProvider extends AbstractTitanGraphComputerProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
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
