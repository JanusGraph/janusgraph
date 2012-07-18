package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DegreeDistribution {

    public static final String LABELS = Tokens.makeNamespace(DegreeDistribution.class) + ".labels";
    public static final String DIRECTION = Tokens.makeNamespace(DegreeDistribution.class) + ".direction";

    public enum Counters {
        EDGES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, IntWritable> {

        private Direction direction;
        private String[] labels;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            long degree = 0l;
            for (final Edge edge : value.getEdges(this.direction, this.labels)) {
                degree++;
            }

            context.getCounter(Counters.EDGES_COUNTED).increment(degree);
            context.write(new LongWritable(degree), new IntWritable(1));

        }

    }

    public static class Reduce extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable> {
        @Override
        public void reduce(final LongWritable key, final Iterable<IntWritable> values, final Reducer<LongWritable, IntWritable, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalDegree = 0l;
            for (final IntWritable token : values) {
                totalDegree++;
            }
            context.write(key, new LongWritable(totalDegree));
        }
    }
}
