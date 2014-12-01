package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BerkeleyGraphProvider extends AbstractTitanGraphProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return BerkeleyStorageSetup.getBerkeleyJEConfiguration(StorageSetup.getHomeDir(graphName));
    }

}
