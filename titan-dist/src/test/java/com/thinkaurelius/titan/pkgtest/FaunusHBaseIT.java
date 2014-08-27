package com.thinkaurelius.titan.pkgtest;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.thinkaurelius.titan.HBaseStorageSetup;

public class FaunusHBaseIT extends AbstractTitanAssemblyIT {

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @Test
    public void testGraphOfTheGods() throws Exception {
        unzipAndRunExpect("faunus-hbase.expect.vm");
    }
}
