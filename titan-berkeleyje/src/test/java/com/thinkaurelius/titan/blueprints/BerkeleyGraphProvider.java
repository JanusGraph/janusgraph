package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJETx;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import java.time.Duration;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BerkeleyGraphProvider extends AbstractTitanGraphProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return BerkeleyStorageSetup.getBerkeleyJEConfiguration(StorageSetup.getHomeDir(graphName)).set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(150L));
    }

    @Override
    public Set<Class> getImplementations() {
        final Set<Class> implementations = super.getImplementations();
        implementations.add(BerkeleyJETx.class);
        return implementations;
    }

}
