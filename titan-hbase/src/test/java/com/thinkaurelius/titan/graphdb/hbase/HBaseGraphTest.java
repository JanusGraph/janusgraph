package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseGraphTest extends TitanGraphTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public HBaseGraphTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration());
    }
}
