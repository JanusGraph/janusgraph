package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanNonTransactionalGraphMetricsTest;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HBaseGraphMetricsTest extends TitanNonTransactionalGraphMetricsTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return HBaseStorageSetup.getHBaseGraphConfiguration();
    }


    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

}
