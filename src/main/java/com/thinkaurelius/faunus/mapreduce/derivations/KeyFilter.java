package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KeyFilter {

    public static final String KEYS = Tokens.makeNamespace(KeyFilter.class) + ".keys";
    public static final String ACTION = Tokens.makeNamespace(KeyFilter.class) + ".action";
    public static final String CLASS = Tokens.makeNamespace(KeyFilter.class) + ".class";

    public enum Counters {
        EDGE_PROPERTIES_KEPT,
        EDGE_PROPERTIES_DROPPED,
        VERTEX_PROPERTIES_KEPT,
        VERTEX_PROPERTIES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Set<String> keys;
        private Tokens.Action action;
        private Class<? extends Element> klass;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final String[] strings = context.getConfiguration().getStrings(KEYS, new String[0]);
            if (strings.length == 0)
                this.keys = new HashSet<String>();
            else
                this.keys = new HashSet<String>(Arrays.asList(strings));
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
            this.klass = context.getConfiguration().getClass(CLASS, Element.class, Element.class);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.klass.equals(Vertex.class)) {
                final Set<String> newKeys = new HashSet<String>(value.getPropertyKeys());
                long originalSize = newKeys.size();
                if (this.action.equals(Tokens.Action.KEEP)) {
                    newKeys.removeAll(this.keys);
                } else {
                    newKeys.retainAll(this.keys);
                }
                
                long newSize = originalSize - newKeys.size();
                for (final String temp : newKeys) {
                    value.removeProperty(temp);
                }

                context.getCounter(Counters.VERTEX_PROPERTIES_DROPPED).increment(originalSize - newSize);
                context.getCounter(Counters.VERTEX_PROPERTIES_KEPT).increment(newSize);
            } else {
                for (final Edge edge : value.getEdges(Direction.BOTH)) {
                    Set<String> newKeys = edge.getPropertyKeys();
                    if (this.action.equals(Tokens.Action.KEEP)) {
                        newKeys.removeAll(this.keys);
                    } else {
                        newKeys.retainAll(this.keys);
                    }
                    for (final String temp : newKeys) {
                        edge.removeProperty(temp);
                    }
                }
            }

            context.write(NullWritable.get(), value);
        }
    }
}
