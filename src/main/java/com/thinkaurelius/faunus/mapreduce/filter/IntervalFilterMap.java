package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.ElementChecker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalFilterMap {

    public static final String CLASS = Tokens.makeNamespace(IntervalFilterMap.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(IntervalFilterMap.class) + ".key";
    public static final String START_VALUE = Tokens.makeNamespace(IntervalFilterMap.class) + ".startValue";
    public static final String END_VALUE = Tokens.makeNamespace(IntervalFilterMap.class) + ".endValue";
    public static final String VALUE_CLASS = Tokens.makeNamespace(IntervalFilterMap.class) + ".valueClass";
    public static final String NULL_WILDCARD = Tokens.makeNamespace(IntervalFilterMap.class) + ".nullWildcard";

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean isVertex;
        private ElementChecker startChecker;
        private ElementChecker endChecker;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            final String key = context.getConfiguration().get(KEY);
            final Class valueClass = context.getConfiguration().getClass(VALUE_CLASS, String.class);
            final Object startValue;
            final Object endValue;
            if (valueClass.equals(String.class)) {
                startValue = context.getConfiguration().get(START_VALUE);
                endValue = context.getConfiguration().get(END_VALUE);
            } else if (Number.class.isAssignableFrom((valueClass))) {
                startValue = context.getConfiguration().getFloat(START_VALUE, Float.MIN_VALUE);
                endValue = context.getConfiguration().getFloat(END_VALUE, Float.MAX_VALUE);
            } else if (valueClass.equals(Boolean.class)) {
                startValue = context.getConfiguration().getBoolean(START_VALUE, false);
                endValue = context.getConfiguration().getBoolean(END_VALUE, true);
            } else {
                throw new IOException("Class " + valueClass + " is an unsupported value class");
            }

            final Boolean nullIsWildcard = context.getConfiguration().getBoolean(NULL_WILDCARD, false);

            this.startChecker = new ElementChecker(nullIsWildcard, key, Query.Compare.GREATER_THAN_EQUAL, startValue);
            this.endChecker = new ElementChecker(nullIsWildcard, key, Query.Compare.LESS_THAN, endValue);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            if (this.isVertex) {
                if (value.hasEnergy() && !(this.startChecker.isLegal(value) && this.endChecker.isLegal(value)))
                    value.setEnergy(0);
            } else {
                for (Edge edge : value.getEdges(Direction.BOTH)) {
                    if (((FaunusEdge) edge).hasEnergy() && !(this.startChecker.isLegal((FaunusEdge)edge) && this.endChecker.isLegal((FaunusEdge)edge)))
                        ((FaunusEdge) edge).setEnergy(0);
                }
            }

            context.write(NullWritable.get(), value);
        }
    }
}
