package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.CounterMap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
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

    public static class Map extends Mapper<NullWritable, FaunusVertex, IntWritable, LongWritable> {

        private Direction direction;
        private String[] labels;
        private CounterMap<Integer> map;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.map = new CounterMap<Integer>();
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, IntWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            int degree = ((List<Edge>) value.getEdges(this.direction, this.labels)).size();
            this.map.incr(degree, 1l);

            context.getCounter(Counters.VERTICES_COUNTED).increment(1);
            context.getCounter(Counters.EDGES_COUNTED).increment(degree);

            // protected against memory explosion
            if (this.map.size() > 1000) {
                this.cleanup(context);
                this.map.clear();
            }
        }


        private final IntWritable intWritable = new IntWritable();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, IntWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (final java.util.Map.Entry<Integer, Long> entry : this.map.entrySet()) {
                this.intWritable.set(entry.getKey());
                this.longWritable.set(entry.getValue());
                context.write(this.intWritable, this.longWritable);
            }
        }

    }

    public static class Reduce extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final IntWritable key, final Iterable<LongWritable> values, final Reducer<IntWritable, LongWritable, IntWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalDegree = 0;
            for (final LongWritable token : values) {
                totalDegree = totalDegree + token.get();
            }
            this.longWritable.set(totalDegree);
            context.write(key, this.longWritable);
        }
    }
}
