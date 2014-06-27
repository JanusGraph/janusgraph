package com.thinkaurelius.titan.hadoop.formats.titan.cassandra;

import com.thinkaurelius.titan.hadoop.formats.titan.cassandra.TitanCassandraInputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanCassandraInputFormat.class.getPackage().getName() + ".*"));
    }
}
