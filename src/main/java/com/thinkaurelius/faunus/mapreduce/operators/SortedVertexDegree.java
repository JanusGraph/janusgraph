package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SortedVertexDegree {

    public static final String LABELS = Tokens.makeNamespace(SortedVertexDegree.class) + ".labels";
    public static final String DIRECTION = Tokens.makeNamespace(SortedVertexDegree.class) + ".direction";
    public static final String PROPERTY = Tokens.makeNamespace(SortedVertexDegree.class) + ".property";
    public static final String ORDER = Tokens.makeNamespace(SortedVertexDegree.class) + ".order";

    private static final String NULL = "null";

    public enum Counters {
        EDGES_COUNTED,
        VERTICES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, IntWritable, FaunusVertex> {

        private Direction direction;
        private String[] labels;
        private Tokens.Order order;


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.order = Tokens.Order.valueOf(context.getConfiguration().get(ORDER));
        }

        private final IntWritable intWritable = new IntWritable();

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, IntWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            int degree = ((List<Edge>) value.getEdges(this.direction, this.labels)).size();
            context.getCounter(Counters.VERTICES_COUNTED).increment(1);
            context.getCounter(Counters.EDGES_COUNTED).increment(degree);
            if (this.order.equals(Tokens.Order.REVERSE))
                this.intWritable.set(degree * -1);
            else
                this.intWritable.set(degree);
            context.write(this.intWritable, value);
        }

    }

    public static class Reduce extends Reducer<IntWritable, FaunusVertex, Text, IntWritable> {

        private String property;
        private Tokens.Order order;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.property = context.getConfiguration().get(PROPERTY);
            this.order = Tokens.Order.valueOf(context.getConfiguration().get(ORDER));
        }

        private final IntWritable intWritable = new IntWritable();
        private final Text textWritable = new Text();

        @Override
        public void reduce(final IntWritable key, final Iterable<FaunusVertex> values, final Reducer<IntWritable, FaunusVertex, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            if (this.order.equals(Tokens.Order.REVERSE))
                this.intWritable.set(key.get() * -1);
            else
                this.intWritable.set(key.get());

            for (final FaunusVertex vertex : values) {
                if (this.property.equals(Tokens._ID))
                    this.textWritable.set(vertex.getId().toString());
                else if (this.property.equals(Tokens._PROPERTIES))
                    this.textWritable.set(vertex.getProperties().toString());
                else {
                    final Object property = vertex.getProperty(this.property);
                    if (null != property)
                        this.textWritable.set(property.toString());
                    else
                        this.textWritable.set(NULL);
                }
                context.write(this.textWritable, this.intWritable);
            }
        }
    }
}
