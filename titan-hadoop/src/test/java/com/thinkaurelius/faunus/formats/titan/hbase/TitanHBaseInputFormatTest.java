package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanHBaseInputFormat.class.getPackage().getName() + ".*"));
    }
}
