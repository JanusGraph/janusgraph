package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class FaunusGraphSONIT extends AbstractTitanAssemblyIT {

    @Test
    public void testGraphOfTheGodsGraphSON() throws Exception {
        unzipAndRunExpect("faunus-graphson.expect.vm", "conf/hadoop/titan-hadoop.properties", "hadoop");
    }
}