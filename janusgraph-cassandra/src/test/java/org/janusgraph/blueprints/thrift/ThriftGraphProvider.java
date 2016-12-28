package com.thinkaurelius.titan.blueprints.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.blueprints.AbstractTitanGraphProvider;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;

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
