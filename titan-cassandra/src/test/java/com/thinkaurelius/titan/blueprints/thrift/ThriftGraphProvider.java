package com.thinkaurelius.titan.blueprints.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.blueprints.AbstractTitanGraphProvider;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ThriftGraphProvider extends AbstractTitanGraphProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return CassandraStorageSetup.getCassandraThriftConfiguration(graphName);
    }

}
