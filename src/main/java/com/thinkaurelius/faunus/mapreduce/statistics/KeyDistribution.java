package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.CounterMap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KeyDistribution {

    public static final String CLASS = Tokens.makeNamespace(KeyDistribution.class) + ".class";

    public enum Counters {
        PROPERTIES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, LongWritable> {

        private Class<? extends Element> klass;
        // making use of in-map aggregation/combiner
        private CounterMap<String> map;


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.map = new CounterMap<String>();
            this.klass = context.getConfiguration().getClass(CLASS, Element.class, Element.class);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            if (this.klass.equals(Vertex.class)) {
                final Set<String> keys = value.getPropertyKeys();
                for (final String temp : keys) {
                    this.map.incr(temp, 1l);
                }
                context.getCounter(Counters.PROPERTIES_COUNTED).increment(keys.size());
            } else {
                for (final Edge edge : value.getEdges(Direction.OUT)) {
                    final Set<String> keys = edge.getPropertyKeys();
                    for (final String temp : keys) {
                        this.map.incr(temp, 1l);
                    }
                    context.getCounter(Counters.PROPERTIES_COUNTED).increment(keys.size());
                }
            }

            // protected against memory explosion
            if (this.map.size() > 1000) {
                this.cleanup(context);
                this.map.clear();
            }

        }

        private final LongWritable longWritable = new LongWritable();
        private final Text textWritable = new Text();

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (final java.util.Map.Entry<String, Long> entry : this.map.entrySet()) {
                this.textWritable.set(null == entry.getKey() ? Tokens.NULL : entry.getKey());
                this.longWritable.set(entry.getValue());
                context.write(this.textWritable, this.longWritable);
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalNumberOfEdges = 0;
            for (final LongWritable token : values) {
                totalNumberOfEdges = totalNumberOfEdges + token.get();
            }
            this.longWritable.set(totalNumberOfEdges);
            context.write(key, this.longWritable);
        }
    }
}
