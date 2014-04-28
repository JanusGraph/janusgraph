package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RexsterInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(RexsterInputFormat.class.getPackage().getName() + ".*"));
    }
}
