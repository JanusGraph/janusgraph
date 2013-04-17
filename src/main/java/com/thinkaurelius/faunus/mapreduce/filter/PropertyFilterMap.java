package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.ElementChecker;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterMap {

    public static final String CLASS = Tokens.makeNamespace(PropertyFilterMap.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(PropertyFilterMap.class) + ".key";
    public static final String VALUES = Tokens.makeNamespace(PropertyFilterMap.class) + ".values";
    public static final String VALUE_CLASS = Tokens.makeNamespace(PropertyFilterMap.class) + ".valueClass";
    public static final String COMPARE = Tokens.makeNamespace(PropertyFilterMap.class) + ".compare";
    public static final String NULL_WILDCARD = Tokens.makeNamespace(PropertyFilterMap.class) + ".nullWildcard";

    public enum Counters {
        VERTICES_FILTERED,
        EDGES_FILTERED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String key, final Query.Compare compare, final Object... values) {
        final String[] valueStrings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            valueStrings[i] = values[i].toString();
        }
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(KEY, key);
        configuration.set(COMPARE, compare.name());
        configuration.setStrings(VALUES, valueStrings);
        configuration.setBoolean(NULL_WILDCARD, false); // TODO: parameterize?
        if (values[0] instanceof String) {
            configuration.setClass(VALUE_CLASS, String.class, String.class);
        } else if (values[0] instanceof Boolean) {
            configuration.setClass(VALUE_CLASS, Boolean.class, Boolean.class);
        } else if (values[0] instanceof Number) {
            configuration.setClass(VALUE_CLASS, Number.class, Number.class);
        } else {
            throw new RuntimeException("Unknown value class: " + values[0].getClass().getName());
        }
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean isVertex;
        private ElementChecker elementChecker;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
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

            if (this.isVertex) {
                if (value.hasPaths() && !this.elementChecker.isLegal(value)) {
                    value.clearPaths();
                    context.getCounter(Counters.VERTICES_FILTERED).increment(1l);
                }
            } else {
                long edgesFiltered = 0;
                for (Edge e : value.getEdges(Direction.BOTH)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths() && !this.elementChecker.isLegal(edge)) {
                        edge.clearPaths();
                        edgesFiltered++;
                    }
                }
                context.getCounter(Counters.EDGES_FILTERED).increment(edgesFiltered);
            }

            context.write(NullWritable.get(), value);
        }
    }
}
