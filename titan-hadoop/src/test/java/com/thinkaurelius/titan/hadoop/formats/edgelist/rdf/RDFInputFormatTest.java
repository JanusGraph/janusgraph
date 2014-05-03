package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.thinkaurelius.titan.hadoop.formats.edgelist.rdf.RDFInputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(RDFInputFormat.class.getPackage().getName() + ".*"));
    }
}
