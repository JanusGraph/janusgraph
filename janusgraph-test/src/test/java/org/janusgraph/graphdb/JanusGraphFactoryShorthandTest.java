package org.janusgraph.graphdb;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.junit.Test;

/**
 * Tests JanusGraphFactory.open's colon-delimited shorthand parameter syntax.
 *
 * This class contains only one method so that it will run in a separate
 * surefire fork.  This is useful for checking acyclic static initializer
 * invocation on the shorthand path (#831).
 */
public class JanusGraphFactoryShorthandTest {

    @Test
    public void testJanusGraphFactoryShorthand() {
        JanusGraph g = JanusGraphFactory.open("inmemory");
        g.close();
    }
}
