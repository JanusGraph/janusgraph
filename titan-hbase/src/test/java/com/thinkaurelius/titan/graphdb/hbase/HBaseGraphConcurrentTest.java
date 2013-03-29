package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseGraphConcurrentTest extends TitanGraphConcurrentTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public HBaseGraphConcurrentTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration());
    }
}
