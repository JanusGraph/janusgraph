package com.thinkaurelius.titan.testutil;

import org.apache.tinkerpop.gremlin.process.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanAssert {

    public static<V extends Element> void assertCount(long expected, GraphTraversal<?,V> traversal) {
        org.junit.Assert.assertEquals(expected, traversal.count().next().longValue());
    }

    public static<V extends Element> void assertEmpty(GraphTraversal<?,V> traversal) {
        org.junit.Assert.assertFalse(traversal.hasNext());
    }

    public static<V extends Element> void assertNotEmpty(GraphTraversal<?,V> traversal) {
        org.junit.Assert.assertTrue(traversal.hasNext());
    }

}
