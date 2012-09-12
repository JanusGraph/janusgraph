package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.ElementPicker;
import com.thinkaurelius.faunus.mapreduce.util.WritableHandler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderMapReduce {

    public static final String CLASS = Tokens.makeNamespace(OrderMapReduce.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(OrderMapReduce.class) + ".key";
    public static final String TYPE = Tokens.makeNamespace(OrderMapReduce.class) + ".type";
    public static final String ELEMENT_KEY = Tokens.makeNamespace(OrderMapReduce.class) + ".elementKey";

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, WritableComparable, Text> {

        private String key;
        private boolean isVertex;
        private WritableHandler handler;
        private String elementKey;


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.key = context.getConfiguration().get(KEY);
            this.handler = new WritableHandler(context.getConfiguration().getClass(TYPE, Text.class, WritableComparable.class));
            this.elementKey = context.getConfiguration().get(ELEMENT_KEY);
        }

        private Text text = new Text();
        private WritableComparable writable;

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, WritableComparable, Text>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    this.text.set(ElementPicker.getPropertyAsString(value, this.elementKey));
                    this.writable = this.handler.set(ElementPicker.getProperty(value, this.key));
                    for (int i = 0; i < value.pathCount(); i++) {
                        context.write(this.writable, this.text);
                    }
                    context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        this.text.set(ElementPicker.getPropertyAsString(edge, this.elementKey));
                        this.writable = this.handler.set(ElementPicker.getProperty(edge, this.key));
                        for (int i = 0; i < edge.pathCount(); i++) {
                            context.write(this.writable, this.text);
                        }
                        edgesProcessed++;
                    }
                }
                context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);
            }
        }
    }

    public static class Reduce extends Reducer<WritableComparable, Text, Text, WritableComparable> {
        @Override
        public void reduce(final WritableComparable key, final Iterable<Text> values, final Reducer<WritableComparable, Text, Text, WritableComparable>.Context context) throws IOException, InterruptedException {
            for (final Text value : values) {
                context.write(value, key);
            }
        }
    }
}
