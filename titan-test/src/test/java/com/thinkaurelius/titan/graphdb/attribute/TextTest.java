package com.thinkaurelius.titan.graphdb.attribute;

import com.thinkaurelius.titan.core.attribute.Cmp;
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
        //Contains
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

        //Prefix
        assertTrue(CONTAINS_PREFIX.evaluate(text, "worl"));
        assertTrue(CONTAINS_PREFIX.evaluate(text, "wORl"));
        assertTrue(CONTAINS_PREFIX.evaluate(text, "ye"));
        assertTrue(CONTAINS_PREFIX.evaluate(text, "Y"));

        assertFalse(CONTAINS_PREFIX.evaluate(text, "fo"));
        assertFalse(CONTAINS_PREFIX.evaluate(text, "of 1f"));
        assertFalse(CONTAINS_PREFIX.evaluate(text, "ses"));


        //Regex
        assertTrue(CONTAINS_REGEX.evaluate(text, "fu[l]+"));
        assertTrue(CONTAINS_REGEX.evaluate(text, "wor[ld]{1,2}"));
        assertTrue(CONTAINS_REGEX.evaluate(text, "\\dfu\\w*"));

        assertFalse(CONTAINS_REGEX.evaluate(text, "fo"));
        assertFalse(CONTAINS_REGEX.evaluate(text, "wor[l]+"));
        assertFalse(CONTAINS_REGEX.evaluate(text, "wor[ld]{3,5}"));


        String name = "fully funny";
        //Cmp
        assertTrue(Cmp.EQUAL.evaluate(name.toString(),name));
        assertFalse(Cmp.NOT_EQUAL.evaluate(name,name));
        assertFalse(Cmp.EQUAL.evaluate("fullly funny",name));
        assertTrue(Cmp.NOT_EQUAL.evaluate("fullly funny",name));

        //Prefix
        assertTrue(PREFIX.evaluate(name,"fully"));
        assertTrue(PREFIX.evaluate(name,"ful"));
        assertTrue(PREFIX.evaluate(name,"fully fu"));
        assertFalse(PREFIX.evaluate(name,"fun"));

        //REGEX
        assertTrue(REGEX.evaluate(name,"(fu[ln]*y) (fu[ln]*y)"));
        assertFalse(REGEX.evaluate(name,"(fu[l]*y) (fu[l]*y)"));
        assertTrue(REGEX.evaluate(name,"(fu[l]*y) .*"));

    }

}
