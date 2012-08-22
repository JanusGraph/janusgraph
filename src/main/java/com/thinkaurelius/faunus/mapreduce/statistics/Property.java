package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.ElementPicker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Property {

    public static final String CLASS = Tokens.makeNamespace(Property.class) + ".class";
    public static final String PROPERTY = Tokens.makeNamespace(Property.class) + ".property";

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, Text> {


        private Class<? extends Element> klass;
        private String property;

        private final Text textWritable = new Text();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.klass = context.getConfiguration().getClass(CLASS, Element.class, Element.class);
            this.property = context.getConfiguration().get(PROPERTY);
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            if (this.klass.equals(Vertex.class)) {
                this.textWritable.set(ElementPicker.getPropertyAsString(value, this.property));
                context.write(NullWritable.get(), this.textWritable);
                context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            } else {
                for (final Edge edge : value.getEdges(Direction.OUT)) {
                    this.textWritable.set(ElementPicker.getPropertyAsString((FaunusEdge) edge, this.property));
                    context.write(NullWritable.get(), this.textWritable);
                    context.getCounter(Counters.EDGES_PROCESSED).increment(1l);
                }
            }
        }
    }
}
