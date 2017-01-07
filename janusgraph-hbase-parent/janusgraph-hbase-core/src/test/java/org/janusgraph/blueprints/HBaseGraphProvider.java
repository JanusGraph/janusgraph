package org.janusgraph.blueprints;

import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HBaseGraphProvider extends AbstractJanusGraphProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return HBaseStorageSetup.getHBaseConfiguration(graphName);
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        try {
            HBaseStorageSetup.startHBase();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return super.openTestGraph(config);
    }
}
