package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.ElementPicker;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.faunus.mapreduce.util.SafeMapperOutputs;
import com.thinkaurelius.faunus.mapreduce.util.WritableHandler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyMap {

    public static final String CLASS = Tokens.makeNamespace(PropertyMap.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(PropertyMap.class) + ".key";
    public static final String TYPE = Tokens.makeNamespace(PropertyMap.class) + ".type";

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String key, final Class<? extends WritableComparable> type) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(KEY, key);
        configuration.setClass(TYPE, type, WritableComparable.class);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, WritableComparable> {

        private String key;
        private boolean isVertex;
        private WritableHandler handler;
        private SafeMapperOutputs outputs;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.key = context.getConfiguration().get(KEY);
            this.handler = new WritableHandler(context.getConfiguration().getClass(TYPE, Text.class, WritableComparable.class));
            this.outputs = new SafeMapperOutputs(context);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, WritableComparable>.Context context) throws IOException, InterruptedException {

            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);

            if (this.isVertex) {
                if (value.hasPaths()) {
                    WritableComparable writable = this.handler.set(ElementPicker.getProperty(value, this.key));
                    for (int i = 0; i < value.pathCount(); i++) {
                        this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), writable);
                    }
                    context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        WritableComparable writable = this.handler.set(ElementPicker.getProperty(edge, this.key));
                        for (int i = 0; i < edge.pathCount(); i++) {
                            this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), writable);
                        }
                        edgesProcessed++;
                    }
                }
                context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);
            }
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, WritableComparable>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }
}
