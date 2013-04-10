package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileInputFormatTest extends TestCase {

    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(FaunusSequenceFileInputFormat.class.getPackage().getName() + ".*"));
    }
}
