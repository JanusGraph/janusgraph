package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Compare;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementCheckerTest extends TestCase {

    public void testEqual() {
        FaunusVertex v1 = new FaunusVertex(1l);
        v1.setProperty("age", 34);

        FaunusVertex v2 = new FaunusVertex(2l);
        v2.setProperty("age", 12);

        FaunusVertex v3 = new FaunusVertex(3l);

        ElementChecker ec = new ElementChecker("age", Compare.EQUAL, 12f, 11f, 15f);
        assertFalse(ec.isLegal(v1));
        assertTrue(ec.isLegal(v2));
        assertFalse(ec.isLegal(v3));
    }

    public void testGreaterThan() {
        FaunusVertex v1 = new FaunusVertex(1l);
        v1.setProperty("age", 34);

        FaunusVertex v2 = new FaunusVertex(2l);
        v2.setProperty("age", 12);

        FaunusVertex v3 = new FaunusVertex(3l);

        ElementChecker ec = new ElementChecker("age", Compare.GREATER_THAN, 20f, 15f, 55f);
        assertTrue(ec.isLegal(v1));
        assertFalse(ec.isLegal(v2));
        assertFalse(ec.isLegal(v3));
    }

    public void testLessThan() {
        FaunusVertex v1 = new FaunusVertex(1l);
        v1.setProperty("age", 34);

        FaunusVertex v2 = new FaunusVertex(2l);
        v2.setProperty("age", 12);

        FaunusVertex v3 = new FaunusVertex(3l);

        ElementChecker ec = new ElementChecker("age", Compare.LESS_THAN, 20f, 15f, 34f);
        assertFalse(ec.isLegal(v1));
        assertTrue(ec.isLegal(v2));
        assertFalse(ec.isLegal(v3));
    }
}
