package com.thinkaurelius.faunus.drivers;

import com.thinkaurelius.faunus.io.formats.json.JSONInputFormat;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementCount extends Configured implements Tool {

    private enum Counter {VERTEX_COUNT, EDGE_COUNT}

    public int run(final String[] args) throws Exception {

        Path in = new Path(args[0]);
        Boolean countVertices = Boolean.valueOf(args[1]);
        Boolean countEdges = Boolean.valueOf(args[2]);

        Configuration config = this.getConf();
        config.setBoolean("countVertices", countVertices);
        config.setBoolean("countEdges", countEdges);

        Job job = new Job(this.getConf(), "Faunus: Element Count");
        job.setJarByClass(ElementCount.class);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, new Path(UUID.randomUUID().toString()));

        job.setMapperClass(GraphIdentity.Map.class);
        job.setInputFormatClass(JSONInputFormat.class);  // TODO
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new ElementCount(), args);
        System.exit(result);
    }

    public static class GraphIdentity {

        public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

            private Boolean countVertices;
            private Boolean countEdges;

            @Override
            public void setup(final Mapper.Context context) throws IOException, InterruptedException {
                this.countVertices = context.getConfiguration().getBoolean("countVertices", true);
                this.countEdges = context.getConfiguration().getBoolean("countEdges", true);
            }

            @Override
            public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
                if (countVertices) {
                    context.getCounter(Counter.VERTEX_COUNT).increment(1);
                }
                if (countEdges) {
                    context.getCounter(Counter.EDGE_COUNT).increment(((List) value.getEdges(Direction.OUT)).size());
                }

            }
        }
    }
}
