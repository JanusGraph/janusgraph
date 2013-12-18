package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraBlueprintsTest extends BasicBlueprintsTest {

    @Override
    protected WriteConfiguration getGraphConfig() {
        return CassandraStorageSetup.getCassandraGraphConfiguration(getClass().getSimpleName());
    }
}
