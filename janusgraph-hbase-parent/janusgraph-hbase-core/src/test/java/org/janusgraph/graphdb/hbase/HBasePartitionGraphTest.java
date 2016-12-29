package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanOperationCountingTest;
import com.thinkaurelius.titan.graphdb.TitanPartitionGraphTest;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HBasePartitionGraphTest extends TitanPartitionGraphTest {

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

}
