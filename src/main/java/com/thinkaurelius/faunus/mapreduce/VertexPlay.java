package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.io.formats.json.FaunusJSONInputFormat;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            for (Edge edge : value.getOutEdges()) {
                context.write(new Text(edge.getLabel()), ONE);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (IntWritable i : values) {
                counter++;
            }
            context.write(key, new IntWritable(counter));
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
        job.setInputFormatClass(FaunusJSONInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new VertexPlay(), args);
        System.exit(result);
    }
}