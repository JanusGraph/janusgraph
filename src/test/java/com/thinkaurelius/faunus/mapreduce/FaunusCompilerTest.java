package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesVerticesMapReduce;
import com.thinkaurelius.faunus.mapreduce.util.CountMapReduce;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusCompilerTest extends TestCase {

    public void testGlobalConfigurations() {
        FaunusGraph graph = new FaunusGraph();
        graph.getConfiguration().setInt("a_property", 2);
        FaunusCompiler compiler = new FaunusCompiler(graph);
        assertEquals(compiler.getConf().getInt("a_property", -1), 2);
        assertEquals(compiler.getConf().getInt("b_property", -1), -1);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());
        compiler.completeSequence();
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("a_property", -1), 2);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("b_property", -1), -1);
        assertEquals(compiler.getConf().getInt("a_property", -1), 2);
        assertEquals(compiler.getConf().getInt("b_property", -1), -1);
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

    public void testJobOrder2() throws Exception {
        FaunusPipeline pipe = new FaunusPipeline(new FaunusGraph());
        FaunusCompiler compiler = pipe.getCompiler();
        assertEquals(compiler.jobs.size(), 0);
        pipe.V().out("knows")._();
        compiler.completeSequence();

        assertEquals(compiler.jobs.size(), 2);

        assertEquals(compiler.jobs.get(0).getMapperClass(), MapSequence.Map.class);
        String[] mapClasses = compiler.jobs.get(0).getConfiguration().getStrings(MapSequence.MAP_CLASSES);
        assertEquals(mapClasses.length, 2);
        assertEquals(mapClasses[0], VerticesMap.Map.class.getName());
        assertEquals(mapClasses[1], VerticesVerticesMapReduce.Map.class.getName());
        assertEquals(compiler.jobs.get(0).getConfiguration().getStrings(VerticesVerticesMapReduce.LABELS + "-1").length, 1);
        assertEquals(compiler.jobs.get(0).getConfiguration().getStrings(VerticesVerticesMapReduce.LABELS + "-1")[0], "knows");
        assertEquals(compiler.jobs.get(0).getCombinerClass(), VerticesVerticesMapReduce.Combiner.class);
        assertEquals(compiler.jobs.get(0).getReducerClass(), VerticesVerticesMapReduce.Reduce.class);

        assertEquals(compiler.jobs.get(1).getMapperClass(), MapSequence.Map.class);
        mapClasses = compiler.jobs.get(1).getConfiguration().getStrings(MapSequence.MAP_CLASSES);
        assertEquals(mapClasses.length, 1);
        assertEquals(mapClasses[0], IdentityMap.Map.class.getName());
        assertEquals(compiler.jobs.get(1).getCombinerClass(), null);
        assertEquals(compiler.jobs.get(1).getReducerClass(), Reducer.class);

    }

    public void testConfigurationPersistence() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("mapred.reduce.tasks", 2);
        conf.setBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, false);
        FaunusGraph graph = new FaunusGraph(conf);
        FaunusPipeline pipeline = new FaunusPipeline(graph);
        FaunusCompiler compiler = pipeline.getCompiler();
        TitanOutputFormat outputFormat = new TitanCassandraOutputFormat();

        assertEquals(graph.getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertEquals(compiler.getConf().getInt("mapred.reduce.tasks", -1), 2);
        assertFalse(graph.getConfiguration().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertFalse(compiler.getConf().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        outputFormat.addMapReduceJobs(compiler);
        assertEquals(compiler.jobs.size(), 1);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertFalse(compiler.jobs.get(0).getConfiguration().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(graph.getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertEquals(compiler.getConf().getInt("mapred.reduce.tasks", -1), 2);

        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, IdentityMap.createConfiguration());
        compiler.completeSequence();
        assertEquals(compiler.jobs.size(), 2);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertFalse(compiler.jobs.get(0).getConfiguration().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(compiler.jobs.get(1).getConfiguration().getInt("mapred.reduce.tasks", -1), 0);
        assertFalse(compiler.jobs.get(1).getConfiguration().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(graph.getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertEquals(compiler.getConf().getInt("mapred.reduce.tasks", -1), 2);
    }
}
