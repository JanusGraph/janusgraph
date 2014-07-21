package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.mapreduce.util.ElementChecker;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
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

    public enum Counters {
        VERTICES_FILTERED,
        EDGES_FILTERED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String key, final Compare compare, final Object... values) {
        final String[] valueStrings = new String[values.length];
        Class valueClass = null;
        for (int i = 0; i < values.length; i++) {
            valueStrings[i] = (null == values[i]) ? valueStrings[i] = null : values[i].toString();
            if (null != values[i])
                valueClass = values[i].getClass();
        }
        if (null == valueClass)
            valueClass = Object.class;


        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(KEY, key);
        configuration.set(COMPARE, compare.name());
        configuration.setStrings(VALUES, valueStrings);
        configuration.setClass(VALUE_CLASS, valueClass, valueClass);
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

            if (valueClass.equals(Object.class)) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = null;
                }
            } else if (valueClass.equals(String.class)) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = (valueStrings[i].equals(Tokens.NULL)) ? null : valueStrings[i];
                }
            } else if (Number.class.isAssignableFrom((valueClass))) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = (valueStrings[i].equals(Tokens.NULL)) ? null : Float.valueOf(valueStrings[i]);
                }
            } else if (valueClass.equals(Boolean.class)) {
                for (int i = 0; i < valueStrings.length; i++) {
                    values[i] = (valueStrings[i].equals(Tokens.NULL)) ? null : Boolean.valueOf(valueStrings[i]);
                }
            } else {
                throw new IOException("Class " + valueClass + " is an unsupported value class");
            }

            final Compare compare = Compare.valueOf(context.getConfiguration().get(COMPARE));
            this.elementChecker = new ElementChecker(key, compare, values);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            if (this.isVertex) {
                if (value.hasPaths() && !this.elementChecker.isLegal(value)) {
                    value.clearPaths();
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_FILTERED, 1L);
                }
            } else {
                long edgesFiltered = 0;
                for (Edge e : value.getEdges(Direction.BOTH)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths() && !this.elementChecker.isLegal(edge)) {
                        edge.clearPaths();
                        edgesFiltered++;
                    }
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_FILTERED, edgesFiltered);
            }

            context.write(NullWritable.get(), value);
        }
    }
}
