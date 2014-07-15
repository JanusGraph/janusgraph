package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EmbeddedBlueprintsTest extends AbstractCassandraBlueprintsTest {

    @Override
    protected WriteConfiguration getGraphConfig() {
        return CassandraStorageSetup.getEmbeddedGraphConfiguration(getClass().getSimpleName());
    }

    @Override
    public void extraCleanUp(String uid) throws BackendException {
        ModifiableConfiguration mc =
                new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, getGraphConfig(), Restriction.NONE);
        StoreManager m = new CassandraEmbeddedStoreManager(mc);
        m.clearStorage();
        m.close();
    }
}
