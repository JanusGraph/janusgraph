package org.janusgraph.graphdb;

import org.janusgraph.core.JanusFactory;
import org.janusgraph.core.JanusGraph;
import org.junit.Test;

/**
 * Tests JanusFactory.open's colon-delimited shorthand parameter syntax.
 *
 * This class contains only one method so that it will run in a separate
 * surefire fork.  This is useful for checking acyclic static initializer
 * invocation on the shorthand path (#831).
 */
public class JanusFactoryShorthandTest {

    @Test
    public void testJanusFactoryShorthand() {
        JanusGraph g = JanusFactory.open("inmemory");
        g.close();
    }
}
