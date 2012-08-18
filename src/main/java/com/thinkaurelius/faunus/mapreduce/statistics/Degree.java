package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.ElementPicker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Degree {

    public static final String LABELS = Tokens.makeNamespace(Degree.class) + ".labels";
    public static final String DIRECTION = Tokens.makeNamespace(Degree.class) + ".direction";
    public static final String PROPERTY = Tokens.makeNamespace(Degree.class) + ".property";

    public enum Counters {
        EDGES_COUNTED,
        VERTICES_COUNTED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, IntWritable> {

        private Direction direction;
        private String[] labels;
        private String property;

        private final IntWritable intWritable = new IntWritable();
        private final Text textWritable = new Text();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.property = context.getConfiguration().get(PROPERTY);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int degree = ((List<Edge>) value.getEdges(this.direction, this.labels)).size();
            context.getCounter(Counters.VERTICES_COUNTED).increment(1);
            context.getCounter(Counters.EDGES_COUNTED).increment(degree);

            this.textWritable.set(ElementPicker.getProperty(value, this.property));
            this.intWritable.set(degree);
            context.write(this.textWritable, this.intWritable);

        }

    }

}
