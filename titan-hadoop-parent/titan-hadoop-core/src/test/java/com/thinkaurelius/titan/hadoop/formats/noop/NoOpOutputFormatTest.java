package com.thinkaurelius.titan.hadoop.formats.noop;

import com.thinkaurelius.titan.hadoop.formats.noop.NoOpOutputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NoOpOutputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(NoOpOutputFormat.class.getPackage().getName() + ".*"));
    }
}
