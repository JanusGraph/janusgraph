package com.thinkaurelius.faunus.graph.runners;

import com.thinkaurelius.faunus.graph.GraphDegreeDistribution;
import com.thinkaurelius.faunus.graph.VertexDegrees;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DegreeDistribution extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        //////// FIRST STEP ////////

        Configuration config1 = this.getConf();
        Job job1 = new Job(config1, "Faunus: Vertex Degrees");
        job1.setJarByClass(VertexDegrees.class);

        Path in1 = new Path(args[0]);
        Path out1 = new Path("/user/marko/tmp.txt");
        FileInputFormat.setInputPaths(job1, in1);
        FileOutputFormat.setOutputPath(job1, out1);

        job1.setMapperClass(VertexDegrees.Map.class);
        job1.setReducerClass(VertexDegrees.Reduce.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.waitForCompletion(true);

        //////// SECOND STEP ////////

        Configuration config2 = this.getConf();
        Job job2 = new Job(config2, "Faunus: Graph Degree Distribution");
        job2.setJarByClass(GraphDegreeDistribution.class);
        Path out2 = new Path(args[1]);
        FileInputFormat.setInputPaths(job2, out1);
        FileOutputFormat.setOutputPath(job2, out2);

        job2.setMapperClass(GraphDegreeDistribution.Map.class);
        job2.setReducerClass(GraphDegreeDistribution.Reduce.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new DegreeDistribution(), args);
        System.exit(result);
    }
}
