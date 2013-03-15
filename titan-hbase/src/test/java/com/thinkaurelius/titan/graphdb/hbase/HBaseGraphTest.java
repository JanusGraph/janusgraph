package com.thinkaurelius.titan.graphdb.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

public class HBaseGraphTest extends TitanGraphTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public HBaseGraphTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration());
    }
}
