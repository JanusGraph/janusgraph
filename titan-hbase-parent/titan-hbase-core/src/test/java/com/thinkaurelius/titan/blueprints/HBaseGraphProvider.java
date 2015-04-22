package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HBaseGraphProvider extends AbstractTitanGraphProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
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
