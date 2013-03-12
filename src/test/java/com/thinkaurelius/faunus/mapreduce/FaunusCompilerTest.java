package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.transform.IdentityMap;
import com.thinkaurelius.faunus.mapreduce.util.CountMapReduce;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusCompilerTest extends TestCase {

    public void testGlobalConfigurations() {
        FaunusGraph graph = new FaunusGraph();
        graph.getConfiguration().setInt("a_property", 2);
        FaunusCompiler compiler = new FaunusCompiler(graph);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());
        compiler.completeSequence();
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("a_property", -1), 2);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("b_property", -1), -1);
    }

    public void testJobListSize() {
        FaunusCompiler compiler = new FaunusCompiler(new FaunusGraph());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 2);
    }

    public void testJobOrder() throws Exception {
        FaunusCompiler compiler = new FaunusCompiler(new FaunusGraph());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);

        assertEquals(compiler.jobs.get(0).getMapperClass(), MapSequence.Map.class);
        assertEquals(compiler.jobs.get(0).getCombinerClass(), null);
        assertEquals(compiler.jobs.get(0).getReducerClass(), CountMapReduce.Reduce.class);
    }
}
