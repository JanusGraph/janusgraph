package org.janusgraph.blueprints;

import org.janusgraph.JanusGraphDatabaseManager;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

public class MultiQueryJanusGraphProvider extends AbstractJanusGraphProvider {
    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return JanusGraphDatabaseManager.getGraphDatabaseProvider().getJanusGraphConfiguration(graphName, test, testMethodName)
            .set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
    }
}
