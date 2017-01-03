package org.janusgraph.pkgtest;

import org.junit.Test;

/**
 * Test the janus.sh script that starts and stops Cassandra, ES, and Gremlin Server.
 */
public class JanusScriptIT extends AbstractJanusAssemblyIT {

    @Test
    public void testGraphOfTheGodsGraphSON() throws Exception {
        unzipAndRunExpect("janus-sh.expect.vm");
    }
}
