package com.thinkaurelius.titan.graphdb.inmemory;

import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryGraphTest extends TitanGraphTest {

    public InMemoryGraphTest() {
        super(getConfiguration());
    }

    public static final Configuration getConfiguration() {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"inmemory");
        return config;
    }

    @Override
    public void clopen() {
        newTx();
    }

    @Override
    public void testTypes() {}

}
