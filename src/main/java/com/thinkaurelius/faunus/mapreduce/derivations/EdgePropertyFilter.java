package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.ElementChecker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgePropertyFilter {

    public static final String KEY = Tokens.makeNamespace(EdgePropertyFilter.class) + ".key";
    public static final String VALUE = Tokens.makeNamespace(EdgePropertyFilter.class) + ".value";
    public static final String VALUE_CLASS = Tokens.makeNamespace(EdgePropertyFilter.class) + ".valueClass";
    public static final String COMPARE = Tokens.makeNamespace(EdgePropertyFilter.class) + ".compare";
    public static final String NULL_WILDCARD = Tokens.makeNamespace(EdgePropertyFilter.class) + ".nullWildcard";

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private ElementChecker elementChecker;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final String key = context.getConfiguration().get(KEY);
            final Class valueClass = context.getConfiguration().getClass(VALUE_CLASS, String.class);
            final Object value;
            if (valueClass.equals(String.class)) {
                value = context.getConfiguration().get(VALUE);
            } else if (Number.class.isAssignableFrom((valueClass))) {
                value = context.getConfiguration().getFloat(VALUE, 0.0f);
            } else if (valueClass.equals(Boolean.class)) {
                value = context.getConfiguration().getBoolean(VALUE, false);
            } else {
                throw new IOException("Class " + valueClass + " is an unsupported value class");
            }

            final Query.Compare compare = Query.Compare.valueOf(context.getConfiguration().get(COMPARE));
            final Boolean nullIsWildcard = context.getConfiguration().getBoolean(NULL_WILDCARD, false);

            this.elementChecker = new ElementChecker(key, compare, value, nullIsWildcard);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final Iterator<Edge> itty = value.getEdges(Direction.BOTH).iterator();
            while (itty.hasNext()) {
                final Edge edge = itty.next();
                if (this.elementChecker.isLegal(edge))
                    context.getCounter(Counters.EDGES_KEPT).increment(1l);
                else {
                    itty.remove();
                    context.getCounter(Counters.EDGES_DROPPED).increment(1l);
                }
            }
            context.write(NullWritable.get(), value);
        }
    }
}
