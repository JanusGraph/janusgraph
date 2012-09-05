package com.thinkaurelius.faunus;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusPipelineTest extends BaseTest {

    public void testElementTypeUpdating() throws IOException {
        FaunusPipeline pipeline = new FaunusPipeline("test", new Configuration());
        try {
            pipeline.outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
        pipeline.v(1, 2, 3, 4).outE("knows").inV().property("key");
        pipeline = new FaunusPipeline("test", new Configuration());
        pipeline.V().E().V().E();


        try {
            pipeline.V().inV();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipeline.E().outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipeline.E().outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineLocking() throws IOException {
        FaunusPipeline pipeline = new FaunusPipeline("test", new Configuration());
        pipeline.V().out().property("name");
        /* // TODO: proper locking required
        try {
            pipeline.V();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }*/

        try {
            pipeline.order(Tokens.Order.INCREASING, "name").V();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineStepIncr() throws IOException {
        FaunusPipeline pipeline = new FaunusPipeline("test", new Configuration());
        assertEquals(pipeline.state.getStep(), -1);
        pipeline.V();
        assertEquals(pipeline.state.getStep(), 0);
        pipeline.as("a");
        assertEquals(pipeline.state.getStep(), 0);
        pipeline.has("name", "marko");
        assertEquals(pipeline.state.getStep(), 0);
        pipeline.out("knows");
        assertEquals(pipeline.state.getStep(), 1);
        pipeline.as("b");
        assertEquals(pipeline.state.getStep(), 1);
        pipeline.outE("battled");
        assertEquals(pipeline.state.getStep(), 2);
        pipeline.as("c");
        assertEquals(pipeline.state.getStep(), 2);       
        pipeline.inV();
        assertEquals(pipeline.state.getStep(), 3);
        pipeline.as("d");
        assertEquals(pipeline.state.getStep(), 3);
        
        assertEquals(pipeline.state.getStep("a"), 0);
        assertEquals(pipeline.state.getStep("b"), 1);
        assertEquals(pipeline.state.getStep("c"), 2);
        assertEquals(pipeline.state.getStep("d"), 3);
    }

}
