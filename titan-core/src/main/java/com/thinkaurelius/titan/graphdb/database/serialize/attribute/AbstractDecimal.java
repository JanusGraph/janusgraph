package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractDecimal extends Number implements Comparable<AbstractDecimal> {

    private static final Logger log =
            LoggerFactory.getLogger(AbstractDecimal.class);

    public static final int MAX_DECIMALS = 9;

    private long format;
    private byte decimals;

    protected AbstractDecimal() {} //For serialization

    public AbstractDecimal(final double value, final int decimals) {
        this(convert(value,decimals),decimals);
    }

    protected AbstractDecimal(final long format, final int decimals) {
        Preconditions.checkArgument(decimals>0 && decimals<=MAX_DECIMALS,"Number of decimals out of range [1-9]: %s",decimals);
        this.decimals=(byte)decimals;
        this.format=format;
    }

    private static long multiplier(final int decimals) {
        long multiplier = 1;
        for (int i = 0; i < decimals; i++) multiplier *= 10;
        return multiplier;
    }

    public static double minDoubleValue(final int decimals) {
        return (Long.MIN_VALUE*1.0 / (multiplier(decimals)+1));
    }

    public static double maxDoubleValue(final int decimals) {
        return (Long.MAX_VALUE*1.0 / (multiplier(decimals)+1));
    }

    public static final boolean withinRange(final double value, final int decimals) {
        return value>=minDoubleValue(decimals) && value<=maxDoubleValue(decimals);
    }

    public static final double EPSILON = 0.00000001;

    public static final long convert(final double value, final int decimals) {
        Preconditions.checkArgument(decimals>0 && decimals<=MAX_DECIMALS,"Number of decimals out of range [1-9]: %s",decimals);
        Preconditions.checkArgument(!Double.isNaN(value), "Value may not be NaN");
        Preconditions.checkArgument(withinRange(value,decimals),"Value out of range for this decimal: %s. Use full precision floating point representation.",value);
        long format = Math.round(value * multiplier(decimals));
        if (Math.abs(convert(format,decimals)-value)> EPSILON) log.warn("Truncated decimals of float value: {}. Use FullFloat for full precision.",value);
        return format;
    }

    public static final double convert(final long format, final int decimals) {
        return ((double) format) / multiplier(decimals);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return format / multiplier(decimals);
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public double doubleValue() {
        return convert(format,decimals);
    }

    public double getDecimalsOnly() {
        long multiplier = multiplier(decimals);
        return ((double)(format % multiplier)/multiplier);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(format).append(decimals).toHashCode();
    }

    @Override
    public String toString() {
        double decimal = getDecimalsOnly();
        assert decimal<1.0 && decimal>=0.0;
        return Long.toString(longValue())+Double.toString(decimal).substring(1);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        else if (other == null) return false;
        else if (other instanceof Number) {
            return doubleValue()==((Number)other).doubleValue();
        } else if (other instanceof AbstractDecimal) {
            AbstractDecimal o = (AbstractDecimal)other;
            return decimals==o.decimals && format==o.format;
        } else return false;
    }

    @Override
    public int compareTo(AbstractDecimal o) {
        return Double.compare(doubleValue(),o.doubleValue());
    }

    public abstract static class AbstractDecimalSerializer<V extends AbstractDecimal> implements OrderPreservingSerializer<V> {

        private final int decimals;
        private final Class<V> type;
        private final LongSerializer ls = new LongSerializer();


        public AbstractDecimalSerializer(final int decimals, final Class<V> type) {
            Preconditions.checkNotNull(type);
            this.type=type;
            Preconditions.checkArgument(decimals>0 && decimals<=MAX_DECIMALS,"Number of decimals out of range [1-9]: %s",decimals);
            this.decimals=decimals;
        }

        protected abstract V construct(final long format, final int decimals);

        @Override
        public V read(ScanBuffer buffer) {
            long format = ls.read(buffer);
            return construct(format,decimals);
        }

        @Override
        public void write(WriteBuffer buffer, V attribute) {
            AbstractDecimal d = attribute;
            Preconditions.checkArgument(d.decimals==decimals,"Invalid argument provided: %s",attribute);
            ls.write(buffer, d.format);
        }

        @Override
        public V readByteOrder(ScanBuffer buffer) {
            return read(buffer);
        }

        @Override
        public void writeByteOrder(WriteBuffer buffer, V attribute) {
            write(buffer,attribute);
        }

        @Override
        public void verifyAttribute(V value) {
            //all are valid
        }

        @Override
        public V convert(Object value) {
            if (value==null) return null;
            else if (value.getClass().equals(type)) return (V)value;
            else if (value instanceof Number) {
                Number n = (Number)value;
                if (AttributeUtil.isWholeNumber(n)) {
                    long l = n.longValue();
                    long multiplier = multiplier(decimals);
                    Preconditions.checkArgument(Long.MIN_VALUE/multiplier<l && Long.MAX_VALUE/multiplier>l,"Number out of range: %s",l);
                    return construct(l*multiplier,decimals);
                } else {
                    return construct(AbstractDecimal.convert(n.doubleValue(), decimals),decimals);
                }
            } else if (value instanceof String) {
                return construct(AbstractDecimal.convert(Double.parseDouble(value.toString()),decimals),decimals);
            } else return null;
        }
    }

}
