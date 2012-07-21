package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexDegree {

    public static final String LABELS = Tokens.makeNamespace(VertexDegree.class) + ".labels";
    public static final String DIRECTION = Tokens.makeNamespace(VertexDegree.class) + ".direction";
    public static final String PROPERTY = Tokens.makeNamespace(VertexDegree.class) + ".property";

    private static final String NULL = "null";

    public enum Counters {
        EDGES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, IntWritable> {

        private Direction direction;
        private String[] labels;
        private String property;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS);
            this.property = context.getConfiguration().get(PROPERTY);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int degree = 0;
            for (final Edge edge : value.getEdges(this.direction, this.labels)) {
                degree++;
            }

            context.getCounter(Counters.EDGES_COUNTED).increment(degree);

            if (this.property.equals(Tokens._ID))
                context.write(new Text(value.getId().toString()), new IntWritable(degree));
            else {
                final Object property = value.getProperty(this.property);
                if (null != property)
                    context.write(new Text(property.toString()), new IntWritable(degree));
                else
                    context.write(new Text(NULL), new IntWritable(degree));
            }

        }

    }

}
