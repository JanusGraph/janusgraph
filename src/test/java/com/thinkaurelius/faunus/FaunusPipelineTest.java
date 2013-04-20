package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.tinkerpop.pipes.transform.TransformPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusPipelineTest extends BaseTest {

    public void testElementTypeUpdating() {
        FaunusPipeline pipe = new FaunusPipeline(new FaunusGraph());
        try {
            pipe.outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
        pipe.v(1, 2, 3, 4).outE("knows").inV().property("key");
        pipe = new FaunusPipeline(new FaunusGraph());
        pipe.V().E().V().E();


        try {
            pipe.V().inV();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipe.E().outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipe.E().outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineLocking() {
        FaunusPipeline pipe = new FaunusPipeline(new FaunusGraph());
        pipe.V().out().property("name");

        try {
            pipe.V();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipe.order(TransformPipe.Order.INCR, "name").V();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineLockingWithMapReduceOutput() throws Exception {
        FaunusGraph graph = new FaunusGraph();
        graph.setGraphOutputFormat(TitanCassandraOutputFormat.class);
        FaunusPipeline pipe = new FaunusPipeline(graph);
        assertFalse(pipe.state.isLocked());
        try {
            pipe.V().out().count().submit();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineStepIncr() {
        FaunusPipeline pipe = new FaunusPipeline(new FaunusGraph());
        assertEquals(pipe.state.getStep(), -1);
        pipe.V();
        assertEquals(pipe.state.getStep(), 0);
        pipe.as("a");
        assertEquals(pipe.state.getStep(), 0);
        pipe.has("name", "marko");
        assertEquals(pipe.state.getStep(), 0);
        pipe.out("knows");
        assertEquals(pipe.state.getStep(), 1);
        pipe.as("b");
        assertEquals(pipe.state.getStep(), 1);
        pipe.outE("battled");
        assertEquals(pipe.state.getStep(), 2);
        pipe.as("c");
        assertEquals(pipe.state.getStep(), 2);
        pipe.inV();
        assertEquals(pipe.state.getStep(), 3);
        pipe.as("d");
        assertEquals(pipe.state.getStep(), 3);

        assertEquals(pipe.state.getStep("a"), 0);
        assertEquals(pipe.state.getStep("b"), 1);
        assertEquals(pipe.state.getStep("c"), 2);
        assertEquals(pipe.state.getStep("d"), 3);
    }

}
