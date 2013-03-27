package com.thinkaurelius.titan.graphdb.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import org.junit.BeforeClass;

public class HBaseGraphPerformanceTest extends TitanGraphPerformanceTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public HBaseGraphPerformanceTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration(), 0, 1, false);
    }
}
