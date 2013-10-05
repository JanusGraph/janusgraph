package com.thinkaurelius.titan.graphdb.attribute;

import org.junit.Test;

import static org.junit.Assert.*;
import static com.thinkaurelius.titan.core.attribute.Text.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TextTest {

    @Test
    public void testContains() {
        String text = "This world is full of 1funny surprises! A Full Yes";
        assertTrue(CONTAINS.evaluate(text, "world"));
        assertTrue(CONTAINS.evaluate(text, "wOrLD"));
        assertFalse(CONTAINS.evaluate(text, "worl"));

        assertTrue(CONTAINS.evaluate(text, "this"));
        assertTrue(CONTAINS.evaluate(text, "yes"));
        assertFalse(CONTAINS.evaluate(text, "funny"));

        assertFalse(CONTAINS.evaluate(text, "a"));
        assertFalse(CONTAINS.evaluate(text, "A"));

        assertTrue(CONTAINS.evaluate(text, "surprises"));
        assertTrue(CONTAINS.evaluate(text, "FULL"));

    }

}
