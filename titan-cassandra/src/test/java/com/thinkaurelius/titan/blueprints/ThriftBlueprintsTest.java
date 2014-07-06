package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

import com.thinkaurelius.titan.CassandraStorageSetup;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ThriftBlueprintsTest extends AbstractCassandraBlueprintsTest {

    @Override
    public void beforeSuite() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    protected WriteConfiguration getGraphConfig() {
        return CassandraStorageSetup.getCassandraGraphConfiguration(getClass().getSimpleName());
    }
}
