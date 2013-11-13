package com.thinkaurelius.titan.graphdb.inmemory;

import com.thinkaurelius.titan.blueprints.TitanSpecificBlueprintsTestSuite;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

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
