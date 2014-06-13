package com.thinkaurelius.titan.hadoop.formats.titan.hbase;

import com.thinkaurelius.titan.hadoop.formats.titan.hbase.TitanHBaseInputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanHBaseInputFormat.class.getPackage().getName() + ".*"));
    }
}
