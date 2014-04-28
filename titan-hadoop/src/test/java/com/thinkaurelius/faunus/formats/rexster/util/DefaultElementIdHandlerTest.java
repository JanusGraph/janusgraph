package com.thinkaurelius.faunus.formats.rexster.util;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultElementIdHandlerTest {
    private DefaultElementIdHandler handler = new DefaultElementIdHandler();
    private TinkerGraph graph = new TinkerGraph();

    @Test
    public void shouldConvertLong() {
        final Vertex v = graph.addVertex(1l);
        Assert.assertEquals(1l, handler.convertIdentifier(v));
    }

    @Test
    public void shouldConvertLongWrapped() {
        final Vertex v = graph.addVertex(new Long(1));
        Assert.assertEquals(1l, handler.convertIdentifier(v));
    }

    @Test
    public void shouldConvertNumeric() {
        final Vertex v = graph.addVertex(1);
        Assert.assertEquals(1l, handler.convertIdentifier(v));
    }

    @Test
    public void shouldConvertString() {
        final Vertex v = graph.addVertex("1");
        Assert.assertEquals(1l, handler.convertIdentifier(v));
    }
}
