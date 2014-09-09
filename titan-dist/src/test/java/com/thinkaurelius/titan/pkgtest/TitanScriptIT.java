package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

/**
 * Test the titan.sh script that starts and stops Cassandra, ES, and Rexster.
 */
public class TitanScriptIT extends AbstractTitanAssemblyIT {

    @Test
    public void testGraphOfTheGodsGraphSON() throws Exception {
        unzipAndRunExpect("titan-sh.expect.vm");
    }
}
