package org.janusgraph.blueprints;

import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;

/**
 * Created by bryn on 06/05/15.
 */
public class InMemoryGraphProvider extends AbstractJanusGraphProvider {
    @Override
    public ModifiableConfiguration getJanusConfiguration(String graphName, Class<?> test, String testMethodName) {
        return StorageSetup.getInMemoryConfiguration();
    }
}
