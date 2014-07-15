package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseCompatLoader;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.tinkerpop.blueprints.Graph;

import java.io.IOException;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
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

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }

    @Override
    public void extraCleanUp(String uid) throws BackendException {
        HBaseStoreManager s = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        s.clearStorage();
        s.close();
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    protected TitanGraph openGraph(String uid) {
        return TitanFactory.open(HBaseStorageSetup.getHBaseGraphConfiguration());
    }
}
