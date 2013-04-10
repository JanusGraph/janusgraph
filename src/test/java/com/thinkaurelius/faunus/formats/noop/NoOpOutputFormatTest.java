package com.thinkaurelius.faunus.formats.noop;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NoOpOutputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(NoOpOutputFormat.class.getPackage().getName() + ".*"));
    }
}
