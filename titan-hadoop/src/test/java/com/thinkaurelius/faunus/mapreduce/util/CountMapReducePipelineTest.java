package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountMapReducePipelineTest extends BaseTest {

    public void testWithPipeline() throws Exception {
        FaunusGraph g = createFaunusGraph(BaseTest.class.getResourceAsStream("graphson-noop.properties"));
        new FaunusPipeline(g).V().count().submit();
        List<String> sideEffect = getSideEffect(TEST_DATA_OUTPUT_PATH + "/job-0/sideeffect-r-00000");
        assertEquals(sideEffect.size(), 1);
        assertEquals(sideEffect.get(0), "12");

        new FaunusPipeline(g).V().has("name", "hercules").count().submit();
        sideEffect = getSideEffect(TEST_DATA_OUTPUT_PATH + "/job-0/sideeffect-r-00000");
        assertEquals(sideEffect.size(), 1);
        assertEquals(sideEffect.get(0), "1");

        new FaunusPipeline(g).V().has("type", "god").count().submit();
        sideEffect = getSideEffect(TEST_DATA_OUTPUT_PATH + "/job-0/sideeffect-r-00000");
        assertEquals(sideEffect.size(), 1);
        assertEquals(sideEffect.get(0), "3");
    }
}
