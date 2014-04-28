package com.thinkaurelius.faunus.formats.script;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptOutputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(ScriptOutputFormat.class.getPackage().getName() + ".*"));
    }
}
