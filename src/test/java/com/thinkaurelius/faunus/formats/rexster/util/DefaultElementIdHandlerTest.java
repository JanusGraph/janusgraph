package com.thinkaurelius.faunus.formats.rexster.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultElementIdHandlerTest {
    private DefaultElementIdHandler handler = new DefaultElementIdHandler();

    @Test
    public void shouldConvertLong() {
        Assert.assertEquals(1l, handler.convertIdentifier(1l));
    }

    @Test
    public void shouldConvertLongWrapped() {
        Assert.assertEquals(1l, handler.convertIdentifier(new Long(1)));
    }

    @Test
    public void shouldConvertNumeric() {
        Assert.assertEquals(1l, handler.convertIdentifier(1));
    }

    @Test
    public void shouldConvertString() {
        Assert.assertEquals(1l, handler.convertIdentifier("1"));
    }
}
