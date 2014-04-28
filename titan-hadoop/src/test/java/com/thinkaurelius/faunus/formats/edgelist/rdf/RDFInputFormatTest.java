package com.thinkaurelius.faunus.formats.edgelist.rdf;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(RDFInputFormat.class.getPackage().getName() + ".*"));
    }
}
