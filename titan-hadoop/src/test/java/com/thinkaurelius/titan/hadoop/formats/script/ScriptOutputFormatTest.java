package com.thinkaurelius.titan.hadoop.formats.script;

import com.thinkaurelius.titan.hadoop.formats.script.ScriptOutputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptOutputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(ScriptOutputFormat.class.getPackage().getName() + ".*"));
    }
}
