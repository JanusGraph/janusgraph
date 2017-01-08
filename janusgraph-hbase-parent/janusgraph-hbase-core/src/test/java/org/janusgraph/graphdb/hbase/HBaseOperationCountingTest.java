package org.janusgraph.graphdb.hbase;

import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HBaseOperationCountingTest extends JanusGraphOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return HBaseStorageSetup.getHBaseGraphConfiguration();
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @Override
    public boolean storeUsesConsistentKeyLocker() {
        return true;
    }

    @Override
    public void testCacheConcurrency() throws InterruptedException {
        //Don't run this test;
    }

}
