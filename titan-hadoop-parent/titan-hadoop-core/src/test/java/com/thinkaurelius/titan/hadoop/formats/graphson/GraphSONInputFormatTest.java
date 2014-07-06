package com.thinkaurelius.titan.hadoop.formats.graphson;

import com.thinkaurelius.titan.hadoop.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(GraphSONInputFormat.class.getPackage().getName() + ".*"));
    }
}
