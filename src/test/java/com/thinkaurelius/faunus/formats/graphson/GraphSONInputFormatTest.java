package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(GraphSONInputFormat.class.getPackage().getName() + ".*"));
    }
}
