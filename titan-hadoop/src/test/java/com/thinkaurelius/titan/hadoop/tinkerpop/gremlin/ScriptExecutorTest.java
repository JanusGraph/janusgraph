package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.ScriptExecutor;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptExecutorTest extends TestCase {

    public void testVariableAllocation() throws Exception {
        ScriptExecutor.evaluate(new FileReader(new File(ScriptExecutorTest.class.getResource("Script.groovy").toURI())), Arrays.<String>asList("this ", "is ", "gremlin"));
    }
}
