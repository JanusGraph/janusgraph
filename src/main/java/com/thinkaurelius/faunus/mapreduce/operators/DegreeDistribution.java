package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DegreeDistribution {

    public static final String LABELS = Tokens.makeNamespace(DegreeDistribution.class) + ".labels";
    public static final String DIRECTION = Tokens.makeNamespace(DegreeDistribution.class) + ".direction";

    public enum Counters {
        VERTICES_COUNTED,
        EDGES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, IntWritable, IntWritable> {

        private Direction direction;
        private String[] labels;
        private java.util.Map<Integer, Integer> map;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.map = new HashMap<Integer, Integer>();
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            int degree = ((List<Edge>) value.getEdges(this.direction, this.labels)).size();

            final Integer count = this.map.get(degree);
            if (null == count)
                this.map.put(degree, 1);
            else
                this.map.put(degree, count + 1);


            context.getCounter(Counters.VERTICES_COUNTED).increment(1);
            context.getCounter(Counters.EDGES_COUNTED).increment(degree);

            // protected against memory explosion
            if (this.map.size() > 1000) {
                this.cleanup(context);
                this.map.clear();
            }
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (final java.util.Map.Entry<Integer, Integer> entry : this.map.entrySet()) {
                context.write(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }

    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            int totalDegree = 0;
            for (final IntWritable token : values) {
                totalDegree = totalDegree + token.get();
            }
            context.write(key, new IntWritable(totalDegree));
        }
    }
}
