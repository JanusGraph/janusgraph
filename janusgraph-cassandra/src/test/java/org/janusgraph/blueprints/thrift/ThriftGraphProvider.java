package org.janusgraph.blueprints.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.blueprints.AbstractTitanGraphProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ThriftGraphProvider extends AbstractTitanGraphProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        CassandraStorageSetup.startCleanEmbedded();
        return CassandraStorageSetup.getCassandraThriftConfiguration(graphName);
    }

}
