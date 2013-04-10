package com.thinkaurelius.faunus.formats.edgelist;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeListOutputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(EdgeListOutputFormat.class.getPackage().getName() + ".*"));
    }
}
