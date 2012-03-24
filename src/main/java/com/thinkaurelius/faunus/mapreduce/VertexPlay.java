package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.io.formats.FaunusTextInputFormat;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexPlay extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, FaunusVertex, FaunusVertex, FaunusVertex> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        public void map(final LongWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<LongWritable, FaunusVertex, FaunusVertex, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.write(value, value);
        }
    }

    public static class Reduce extends Reducer<FaunusVertex, FaunusVertex, Text, LongWritable> {

        @Override
        public void reduce(final FaunusVertex key, final Iterable<FaunusVertex> values, final org.apache.hadoop.mapreduce.Reducer<FaunusVertex, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (final FaunusVertex i : values) {
                counter++;
            }
            context.write(new Text(key.getId() + ":" + key.getProperty("blop").toString()), new LongWritable(counter));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration config = this.getConf();
        Job job = new Job(config, "Faunus: Vertex Play");
        job.setJarByClass(VertexPlay.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(FaunusTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper output
        job.setOutputKeyClass(FaunusVertex.class);
        job.setOutputValueClass(FaunusVertex.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new VertexPlay(), args);
        System.exit(result);
    }
}