package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class HBaseGraphComputerProvider extends AbstractTitanGraphComputerProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        ModifiableConfiguration config = super.getTitanConfiguration(graphName, test, testMethodName);
        config.setAll(HBaseStorageSetup.getHBaseConfiguration(graphName).getAll());
        return config;
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
