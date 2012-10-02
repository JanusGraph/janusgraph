package com.thinkaurelius.titan.tinkerpop.gremlin;

import junit.framework.TestCase;

import java.io.StringReader;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptExecutorTest extends TestCase {

    public void testProperImports() throws Exception {
        ScriptExecutor.evaluate(new StringReader("TitanFactory.class"), (List) Collections.emptyList());
    }
}
