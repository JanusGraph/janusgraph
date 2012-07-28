package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
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
import java.util.HashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyDistribution {

    public static final String PROPERTY = Tokens.makeNamespace(PropertyDistribution.class) + ".property";
    public static final String CLASS = Tokens.makeNamespace(PropertyDistribution.class) + ".class";
    private static final String NULL = "null";

    public enum Counters {
        PROPERTIES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, LongWritable> {

        private String property;
        private Class<? extends Element> klass;
        // making use of in-map aggregation/combiner
        private java.util.Map<String, Long> map;


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.map = new HashMap<String, Long>();
            this.property = context.getConfiguration().get(PROPERTY);
            try {
                this.klass = (Class<? extends Element>) Class.forName(context.getConfiguration().get(CLASS));
            } catch (ClassNotFoundException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            if (this.klass.equals(Vertex.class)) {
                final Object temp = value.getProperty(this.property);
                final String propertyValue = null == temp ? NULL : temp.toString();
                final Long count = this.map.get(propertyValue);
                if (null == count)
                    this.map.put(propertyValue, 1l);
                else
                    this.map.put(propertyValue, count + 1l);

                context.getCounter(Counters.PROPERTIES_COUNTED).increment(1l);
            } else {
                for(final Edge edge : value.getEdges(Direction.OUT)) {
                    final Object temp = edge.getProperty(this.property);
                    final String propertyValue = null == temp ? NULL : temp.toString();
                    final Long count = this.map.get(propertyValue);
                    if (null == count)
                        this.map.put(propertyValue, 1l);
                    else
                        this.map.put(propertyValue, count + 1l);

                    context.getCounter(Counters.PROPERTIES_COUNTED).increment(1l);
                }
            }

            // protected against memory explosion
            if (this.map.size() > 10000) {
                this.cleanup(context);
                this.map.clear();
            }

        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (final java.util.Map.Entry<String, Long> entry : map.entrySet()) {
                context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalNumberOfEdges = 0;
            for (final LongWritable token : values) {
                totalNumberOfEdges = totalNumberOfEdges + token.get();
            }
            context.write(key, new LongWritable(totalNumberOfEdges));
        }
    }
}
