package com.thinkaurelius.faunus.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OrientElementIdHandlerTest {
    private final OrientElementIdHandler elementIdHandler = new OrientElementIdHandler();

    @Test
    public void shouldConvertRid() {
        Assert.assertEquals(321l, elementIdHandler.convertIdentifier("#123:321"));
    }
}
