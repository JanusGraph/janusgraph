package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.transform.IdentityMap;
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

    }
}
