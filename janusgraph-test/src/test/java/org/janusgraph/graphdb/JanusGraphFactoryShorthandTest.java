package org.janusgraph.graphdb;

import org.janusgraph.core.TitanFactory;
import org.janusgraph.core.TitanGraph;
import org.junit.Test;

/**
 * Tests TitanFactory.open's colon-delimited shorthand parameter syntax.
 *
 * This class contains only one method so that it will run in a separate
 * surefire fork.  This is useful for checking acyclic static initializer
 * invocation on the shorthand path (#831).
 */
public class TitanFactoryShorthandTest {

    @Test
    public void testTitanFactoryShorthand() {
        TitanGraph g = TitanFactory.open("inmemory");
        g.close();
    }
}
