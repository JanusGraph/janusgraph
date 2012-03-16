package com.thinkaurelius.faunus.graph.mapreduce;

import com.thinkaurelius.faunus.graph.mapreduce.VertexDegrees;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphDegreeDistribution extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, final org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.valueOf(value.toString().split("\t")[1])), ONE);
        }
    }

    public static class Reduce extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable> {

        @Override
        public void reduce(final LongWritable key, final Iterable<IntWritable> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, IntWritable, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (final IntWritable i : values) {
                counter++;
            }
            context.write(key, new LongWritable(counter));
        }
    }


    public int run(String[] args) throws Exception {
        Configuration config = this.getConf();
        Job job = new Job(config, "Faunus: Graph Degree Distribution");
        job.setJarByClass(VertexDegrees.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper output
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new VertexDegrees(), args);
        System.exit(result);
    }
}