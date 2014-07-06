package com.thinkaurelius.titan.hadoop.mapreduce.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WritableHandler {

    public Class<? extends WritableComparable> type;

    private Text text = new Text();
    private LongWritable longWritable = new LongWritable();
    private IntWritable intWritable = new IntWritable();
    private FloatWritable floatWritable = new FloatWritable();
    private DoubleWritable doubleWritable = new DoubleWritable();

    private static final Text NULL_TEXT = new Text("null");
    private static final LongWritable NULL_LONG = new LongWritable(Long.MIN_VALUE);
    private static final IntWritable NULL_INT = new IntWritable(Integer.MIN_VALUE);
    private static final FloatWritable NULL_FLOAT = new FloatWritable(Float.NaN);
    private static final DoubleWritable NULL_DOUBLE = new DoubleWritable(Double.NaN);

    public WritableHandler(final Class<? extends WritableComparable> type) {
        this.type = type;
        // if (!type.equals(Text.class) && !type.equals(LongWritable.class) && !type.equals(IntWritable.class) && !type.equals(FloatWritable.class) && !type.equals(DoubleWritable.class))
        //    throw new IllegalArgumentException("The provided type is not supported: " + type.getName());
    }

    public WritableComparable set(final String s) {
        if (null == s) return NULL_TEXT;

        if (type.equals(LongWritable.class)) {
            longWritable.set(Long.valueOf(s));
            return longWritable;
        } else if (type.equals(IntWritable.class)) {
            intWritable.set(Integer.valueOf(s));
            return intWritable;
        } else if (type.equals(DoubleWritable.class)) {
            doubleWritable.set(Double.valueOf(s));
            return doubleWritable;
        } else if (type.equals(FloatWritable.class)) {
            floatWritable.set(Float.valueOf(s));
            return floatWritable;
        } else {
            text.set(s);
            return text;
        }
    }

    public WritableComparable set(final Long l) {
        if (null == l) return NULL_LONG;

        if (type.equals(LongWritable.class)) {
            longWritable.set(l);
            return longWritable;
        } else if (type.equals(IntWritable.class)) {
            intWritable.set(l.intValue());
            return intWritable;
        } else if (type.equals(DoubleWritable.class)) {
            doubleWritable.set(l.doubleValue());
            return doubleWritable;
        } else if (type.equals(FloatWritable.class)) {
            floatWritable.set(l.floatValue());
            return floatWritable;
        } else {
            text.set(String.valueOf(l));
            return text;
        }
    }

    public WritableComparable set(final Integer i) {
        if (null == i) return NULL_INT;

        if (type.equals(LongWritable.class)) {
            longWritable.set(i.longValue());
            return longWritable;
        } else if (type.equals(IntWritable.class)) {
            intWritable.set(i);
            return intWritable;
        } else if (type.equals(DoubleWritable.class)) {
            doubleWritable.set(i.doubleValue());
            return doubleWritable;
        } else if (type.equals(FloatWritable.class)) {
            floatWritable.set(i.floatValue());
            return floatWritable;
        } else {
            text.set(String.valueOf(i));
            return text;
        }
    }

    public WritableComparable set(final Double d) {
        if (null == d) return NULL_DOUBLE;

        if (type.equals(LongWritable.class)) {
            longWritable.set(d.longValue());
            return longWritable;
        } else if (type.equals(IntWritable.class)) {
            intWritable.set(d.intValue());
            return intWritable;
        } else if (type.equals(DoubleWritable.class)) {
            doubleWritable.set(d);
            return doubleWritable;
        } else if (type.equals(FloatWritable.class)) {
            floatWritable.set(d.floatValue());
            return floatWritable;
        } else {
            text.set(String.valueOf(d));
            return text;
        }
    }

    public WritableComparable set(final Float f) {
        if (null == f) return NULL_FLOAT;

        if (type.equals(LongWritable.class)) {
            longWritable.set(f.longValue());
            return longWritable;
        } else if (type.equals(IntWritable.class)) {
            intWritable.set(f.intValue());
            return intWritable;
        } else if (type.equals(DoubleWritable.class)) {
            doubleWritable.set(f.doubleValue());
            return doubleWritable;
        } else if (type.equals(FloatWritable.class)) {
            floatWritable.set(f);
            return floatWritable;
        } else {
            text.set(String.valueOf(f));
            return text;
        }
    }

    public WritableComparable set(final Object object) {
        if (null == object) {
            if (type.equals(Text.class))
                return NULL_TEXT;
            else if (type.equals(LongWritable.class))
                return NULL_LONG;
            else if (type.equals(IntWritable.class))
                return NULL_INT;
            else if (type.equals(DoubleWritable.class))
                return NULL_DOUBLE;
            else if (type.equals(FloatWritable.class))
                return NULL_FLOAT;
            else
                return NULL_TEXT;
        } else {
            if (object instanceof Long)
                return set((Long) object);
            else if (object instanceof Integer)
                return set((Integer) object);
            else if (object instanceof Double)
                return set((Double) object);
            else if (object instanceof Float)
                return set((Float) object);
            else
                return set(object.toString());
        }
    }
}
