package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseGraphTest extends TitanGraphTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return HBaseStorageSetup.getHBaseGraphConfiguration();
    }

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

}
