package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusEdge;
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
public class EdgeValueFilter {

    public static final String KEY = Tokens.makeNamespace(EdgeValueFilter.class) + ".key";
    public static final String VALUES = Tokens.makeNamespace(EdgeValueFilter.class) + ".values";
    public static final String VALUE_CLASS = Tokens.makeNamespace(EdgeValueFilter.class) + ".valueClass";
    public static final String COMPARE = Tokens.makeNamespace(EdgeValueFilter.class) + ".compare";
    public static final String NULL_WILDCARD = Tokens.makeNamespace(EdgeValueFilter.class) + ".nullWildcard";

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
            final String[] valueStrings = context.getConfiguration().getStrings(VALUES);
            final Object[] values = new Object[valueStrings.length];
            if (valueClass.equals(String.class)) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = valueStrings[i];
                }
            } else if (Number.class.isAssignableFrom((valueClass))) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = Float.valueOf(valueStrings[i]);
                }
            } else if (valueClass.equals(Boolean.class)) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = Boolean.valueOf(valueStrings[i]);
                }
            } else {
                throw new IOException("Class " + valueClass + " is an unsupported value class");
            }

            final Query.Compare compare = Query.Compare.valueOf(context.getConfiguration().get(COMPARE));
            final Boolean nullIsWildcard = context.getConfiguration().getBoolean(NULL_WILDCARD, false);

            this.elementChecker = new ElementChecker(nullIsWildcard, key, compare, values);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final Iterator<Edge> itty = value.getEdges(Direction.BOTH).iterator();
            while (itty.hasNext()) {
                final Edge edge = itty.next();
                if (this.elementChecker.isLegal((FaunusEdge) edge))
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
