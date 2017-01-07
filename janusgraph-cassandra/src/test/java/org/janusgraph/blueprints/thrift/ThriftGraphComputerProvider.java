package org.janusgraph.blueprints.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.blueprints.AbstractJanusGraphComputerProvider;
import org.janusgraph.blueprints.AbstractJanusGraphProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.GraphProvider;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class ThriftGraphComputerProvider extends AbstractJanusGraphComputerProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        CassandraStorageSetup.startCleanEmbedded();
        return CassandraStorageSetup.getCassandraThriftConfiguration(graphName);
    }

}
