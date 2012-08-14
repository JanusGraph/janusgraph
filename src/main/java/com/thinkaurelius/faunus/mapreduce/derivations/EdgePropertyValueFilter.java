package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgePropertyValueFilter {

    public static final String KEY = Tokens.makeNamespace(EdgePropertyValueFilter.class) + ".key";
    public static final String VALUE = Tokens.makeNamespace(EdgePropertyValueFilter.class) + ".value";
    public static final String VALUE_CLASS = Tokens.makeNamespace(EdgePropertyValueFilter.class) + ".valueClass";
    public static final String COMPARE = Tokens.makeNamespace(EdgePropertyValueFilter.class) + ".compare";

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String key;
        private Object value;
        private Query.Compare compare;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.key = context.getConfiguration().get(KEY);
            final Class valueClass = context.getConfiguration().getClass(VALUE_CLASS, String.class);
            if (valueClass.equals(String.class)) {
                this.value = context.getConfiguration().get(VALUE);
            } else if (Number.class.isAssignableFrom((valueClass))) {
                this.value = context.getConfiguration().getFloat(VALUE, 0.0f);
            } else if (valueClass.equals(Boolean.class)) {
                this.value = context.getConfiguration().getBoolean(VALUE, false);
            } else {
                throw new IOException("Class " + valueClass + " is an unsupported value class");
            }

            this.compare = Query.Compare.valueOf(context.getConfiguration().get(COMPARE));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final Iterator<Edge> itty = value.getEdges(Direction.BOTH).iterator();
            while (itty.hasNext()) {
                final Edge edge = itty.next();
                if (this.isLegal(edge))
                    context.getCounter(Counters.EDGES_KEPT).increment(1l);
                else {
                    itty.remove();
                    context.getCounter(Counters.EDGES_DROPPED).increment(1l);
                }
            }
            context.write(NullWritable.get(), value);
        }

        private boolean isLegal(final Element element) {
            Object elementValue = element.getProperty(this.key);
            if (elementValue instanceof Number)
                elementValue = ((Number) elementValue).floatValue();

            switch (this.compare) {
                case EQUAL:
                    if (null == elementValue)
                        return this.value == null;
                    return elementValue.equals(this.value);
                case NOT_EQUAL:
                    if (null == elementValue)
                        return this.value != null;
                    return !elementValue.equals(this.value);
                case GREATER_THAN:
                    if (null == elementValue || this.value == null)
                        return false;
                    return ((Comparable) elementValue).compareTo(this.value) >= 1;
                case LESS_THAN:
                    if (null == elementValue || this.value == null)
                        return false;
                    return ((Comparable) elementValue).compareTo(this.value) <= -1;
                case GREATER_THAN_EQUAL:
                    if (null == elementValue || this.value == null)
                        return false;
                    return ((Comparable) elementValue).compareTo(this.value) >= 0;
                case LESS_THAN_EQUAL:
                    if (null == elementValue || this.value == null)
                        return false;
                    return ((Comparable) elementValue).compareTo(this.value) <= 0;
                default:
                    throw new IllegalArgumentException("Invalid state as no valid filter was provided");
            }
        }
    }
}
