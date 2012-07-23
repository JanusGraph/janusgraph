package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeLabelDistribution {

    public static final String DIRECTION = Tokens.makeNamespace(EdgeLabelDistribution.class) + ".direction";

    public enum Counters {
        EDGES_COUNTED,
        VERTICES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, IntWritable> {

        private Direction direction;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (final Edge edge : value.getEdges(this.direction)) {
                counter++;
                context.write(new Text(edge.getLabel()), new IntWritable(1));
            }
            context.getCounter(Counters.VERTICES_COUNTED).increment(1);
            context.getCounter(Counters.EDGES_COUNTED).increment(counter);
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int totalNumberOfEdges = 0;
            for (final IntWritable token : values) {
                totalNumberOfEdges++;
            }
            context.write(key, new IntWritable(totalNumberOfEdges));
        }
    }
}
