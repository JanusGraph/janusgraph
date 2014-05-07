package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopPipeline;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountMapReducePipelineTest extends BaseTest {

    public void testWithPipeline() throws Exception {
        HadoopGraph g = createHadoopGraph(BaseTest.class.getResourceAsStream("graphson-noop.properties"));
        new HadoopPipeline(g).V().count().submit();
        List<String> sideEffect = getSideEffect(TEST_DATA_OUTPUT_PATH + "/job-0/sideeffect-r-00000");
        assertEquals(sideEffect.size(), 1);
        assertEquals(sideEffect.get(0), "12");

        new HadoopPipeline(g).V().has("name", "hercules").count().submit();
        sideEffect = getSideEffect(TEST_DATA_OUTPUT_PATH + "/job-0/sideeffect-r-00000");
        assertEquals(sideEffect.size(), 1);
        assertEquals(sideEffect.get(0), "1");

        new HadoopPipeline(g).V().has("type", "god").count().submit();
        sideEffect = getSideEffect(TEST_DATA_OUTPUT_PATH + "/job-0/sideeffect-r-00000");
        assertEquals(sideEffect.size(), 1);
        assertEquals(sideEffect.get(0), "3");
    }
}
