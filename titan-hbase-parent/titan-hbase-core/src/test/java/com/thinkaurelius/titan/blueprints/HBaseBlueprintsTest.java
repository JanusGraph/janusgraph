package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

import java.io.IOException;
/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HBaseBlueprintsTest extends TitanBlueprintsTest {

    @Override
    public void beforeSuite() {
        try {
            HBaseStorageSetup.startHBase();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void afterSuite() {
        // we don't need to restart on each test because cleanup is in please
    }

    @Override
    public Graph generateGraph() {
        return TitanFactory.open(HBaseStorageSetup.getHBaseGraphConfiguration());
    }

    @Override
    public void cleanUp() throws StorageException {
        HBaseStoreManager s = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        s.clearStorage();
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }
}
