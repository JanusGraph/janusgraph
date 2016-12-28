package org.janusgraph.pkgtest;

import org.junit.Test;

public class GremlinShellArgsIT extends AbstractTitanAssemblyIT {

    @Test
    public void testScriptFileArgument() throws Exception {
        unzipAndRunExpect("gremlin-shell-args.expect.vm");
    }
}
