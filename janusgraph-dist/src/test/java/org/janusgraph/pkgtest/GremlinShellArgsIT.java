package org.janusgraph.pkgtest;

import org.junit.Test;

public class GremlinShellArgsIT extends AbstractJanusGraphAssemblyIT {

    @Test
    public void testScriptFileArgument() throws Exception {
        unzipAndRunExpect("gremlin-shell-args.expect.vm");
    }
}
