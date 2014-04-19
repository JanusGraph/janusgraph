package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.olap.FulgoraOLAPTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseOLAPTest extends FulgoraOLAPTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return HBaseStorageSetup.getHBaseGraphConfiguration();
    }

}
