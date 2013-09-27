package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public HBaseGraphPerformanceMemoryTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration());
    }
}
