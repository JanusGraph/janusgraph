package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.CounterMap;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AdjacentVertexProperties {

    public static final String PROPERTY = Tokens.makeNamespace(AdjacentVertexProperties.class) + ".property";
    private static final String NULL = "null";

    public enum Counters {
        EDGES_COUNTED,
        VERTICES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            context.write(value.getIdAsLongWritable(), new Holder<FaunusVertex>('f', value));
            for (final Edge edge : value.getEdges(OUT)) {
                final FaunusVertex vertexB = (FaunusVertex) edge.getVertex(IN);
                context.write(vertexB.getIdAsLongWritable(), new Holder<FaunusVertex>('r', value));
                counter++;
            }
            context.getCounter(Counters.EDGES_COUNTED).increment(counter);
            context.getCounter(Counters.VERTICES_COUNTED).increment(1l);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, Text, Text> {

        private String property;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.property = context.getConfiguration().get(PROPERTY);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, Text, Text>.Context context) throws IOException, InterruptedException {
            final Text fText = new Text();
            final List<Text> thusFar = new ArrayList<Text>();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 'f') {
                    fText.set(toStringProperty(holder.get().getProperty(this.property)));
                    break;
                } else {
                    thusFar.add(new Text(toStringProperty(holder.get().getProperty(this.property))));
                }
            }
            for (final Text rText : thusFar) {
                context.write(rText, fText);
            }
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 'r') {
                    context.write(new Text(toStringProperty(holder.get().getProperty(this.property))), fText);
                }
            }
        }

        private String toStringProperty(final Object propertyValue) {
            return null == propertyValue ? NULL : propertyValue.toString();
        }
    }

    public static class Map2 extends Mapper<Text, Text, Text, LongWritable> {

        private CounterMap<String> map;

        @Override
        public void setup(final Mapper<Text, Text, Text, LongWritable>.Context context) {
            this.map = new CounterMap<String>();
        }

        @Override
        public void map(final Text key, final Text value, final Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            final String newKey = "(" + key.toString() + "," + value.toString() + ")";
            this.map.incr(newKey, 1l);

            if (this.map.size() > 10000) {
                this.cleanup(context);
                this.map.clear();
            }
        }

        @Override
        public void cleanup(final Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (final java.util.Map.Entry<String, Long> entry : this.map.entrySet()) {
                context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
            }
        }

    }

    public static class Reduce2 extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (final LongWritable value : values) {
                counter = counter + value.get();
            }
            context.write(key, new LongWritable(counter));
        }
    }
}
