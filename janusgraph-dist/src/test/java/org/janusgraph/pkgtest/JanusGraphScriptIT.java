package org.janusgraph.pkgtest;

import org.junit.Test;

/**
 * Test the janusgraph.sh script that starts and stops Cassandra, ES, and Gremlin Server.
 */
public class JanusGraphScriptIT extends AbstractJanusGraphAssemblyIT {

    @Test
    public void testGraphOfTheGodsGraphSON() throws Exception {
        unzipAndRunExpect("janusgraph-sh.expect.vm");
    }
}
