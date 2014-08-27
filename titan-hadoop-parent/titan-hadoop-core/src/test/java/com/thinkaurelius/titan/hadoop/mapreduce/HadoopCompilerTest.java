package com.thinkaurelius.titan.hadoop.mapreduce;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopCompilerTest extends TestCase {

    public void testTrue() {
    }

    /* public void testGlobalConfigurations() {
        HadoopGraph graph = new HadoopGraph();
        graph.getConf().setInt("a_property", 2);
        HadoopCompiler compiler = new HadoopCompiler(graph);
        assertEquals(compiler.getConf().getInt("a_property", -1), 2);
        assertEquals(compiler.getConf().getInt("b_property", -1), -1);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, HadoopVertex.class, new Configuration());
        //compiler.completeSequence();
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("a_property", -1), 2);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("b_property", -1), -1);
        assertEquals(compiler.getConf().getInt("a_property", -1), 2);
        assertEquals(compiler.getConf().getInt("b_property", -1), -1);
    } */

    /* public void testJobListSize() {
        HadoopCompiler compiler = new HadoopCompiler(new HadoopGraph());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, HadoopVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, HadoopVertex.class, NullWritable.class, HadoopVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, HadoopVertex.class, NullWritable.class, HadoopVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 2);
    } */

    /* public void testJobOrder() throws Exception {
        HadoopCompiler compiler = new HadoopCompiler(new HadoopGraph());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, HadoopVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, HadoopVertex.class, NullWritable.class, HadoopVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);

        assertEquals(compiler.jobs.get(0).getMapperClass(), MapSequence.Map.class);
        assertEquals(compiler.jobs.get(0).getCombinerClass(), null);
        assertEquals(compiler.jobs.get(0).getReducerClass(), CountMapReduce.Reduce.class);
    } */

    /* public void testJobOrder2() throws Exception {
        HadoopPipeline pipe = new HadoopPipeline(new HadoopGraph());
        HadoopCompiler compiler = pipe.getCompiler();
        assertEquals(compiler.jobs.size(), 0);
        pipe.V().out("knows")._();
        //compiler.completeSequence();

        assertEquals(compiler.jobs.size(), 2);

        assertEquals(compiler.jobs.get(0).getMapperClass(), MapSequence.Map.class);
        String[] mapClasses = compiler.jobs.get(0).getConfiguration().getStrings(MapSequence.MAP_CLASSES);
        assertEquals(mapClasses.length, 2);
        assertEquals(mapClasses[0], VerticesMap.Map.class.getName());
        assertEquals(mapClasses[1], VerticesVerticesMapReduce.Map.class.getName());
        assertEquals(compiler.jobs.get(0).getConfiguration().getStrings(VerticesVerticesMapReduce.LABELS + "-1").length, 1);
        assertEquals(compiler.jobs.get(0).getConfiguration().getStrings(VerticesVerticesMapReduce.LABELS + "-1")[0], "knows");
        assertEquals(compiler.jobs.get(0).getCombinerClass(), null);
        assertEquals(compiler.jobs.get(0).getReducerClass(), VerticesVerticesMapReduce.Reduce.class);

        assertEquals(compiler.jobs.get(1).getMapperClass(), MapSequence.Map.class);
        mapClasses = compiler.jobs.get(1).getConfiguration().getStrings(MapSequence.MAP_CLASSES);
        assertEquals(mapClasses.length, 1);
        assertEquals(mapClasses[0], IdentityMap.Map.class.getName());
        assertEquals(compiler.jobs.get(1).getCombinerClass(), null);
        assertEquals(compiler.jobs.get(1).getReducerClass(), Reducer.class);

    } */

    /* public void testConfigurationPersistence() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("mapred.reduce.tasks", 2);
        conf.setBoolean(TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, false);
        HadoopGraph graph = new HadoopGraph(conf);
        HadoopPipeline pipeline = new HadoopPipeline(graph);
        HadoopCompiler compiler = pipeline.getCompiler();
        TitanOutputFormat outputFormat = new TitanCassandraOutputFormat();

        assertEquals(graph.getConf().getInt("mapred.reduce.tasks", -1), 2);
        assertEquals(compiler.getConf().getInt("mapred.reduce.tasks", -1), 2);
        assertFalse(graph.getConf().getBoolean(TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertFalse(compiler.getConf().getBoolean(TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        outputFormat.addMapReduceJobs(compiler);
        assertEquals(compiler.jobs.size(), 1);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertFalse(compiler.jobs.get(0).getConfiguration().getBoolean(TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(graph.getConf().getInt("mapred.reduce.tasks", -1), 2);
        assertEquals(compiler.getConf().getInt("mapred.reduce.tasks", -1), 2);

        compiler.addMap(IdentityMap.Map.class, NullWritable.class, HadoopVertex.class, IdentityMap.createConfiguration());
        //compiler.completeSequence();
        assertEquals(compiler.jobs.size(), 2);
        assertEquals(compiler.jobs.get(0).getConfiguration().getInt("mapred.reduce.tasks", -1), 2);
        assertFalse(compiler.jobs.get(0).getConfiguration().getBoolean(TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(compiler.jobs.get(1).getConfiguration().getInt("mapred.reduce.tasks", -1), 0);
        assertFalse(compiler.jobs.get(1).getConfiguration().getBoolean(TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(graph.getConf().getInt("mapred.reduce.tasks", -1), 2);
        assertEquals(compiler.getConf().getInt("mapred.reduce.tasks", -1), 2);
    } */
}
