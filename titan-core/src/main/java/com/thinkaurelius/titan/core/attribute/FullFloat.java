package com.thinkaurelius.titan.core.attribute;

/**
 * Helper class to be used when wanting to store the full value spectrum of a float value.
 * <p/>
 * By default, Titan will convert float values into a decimal format so that it can be stored in byte-order
 * and be used for vertex-centric indices. However, that restricts the value spectrum of floats when registered
 * using {@link com.thinkaurelius.titan.core.KeyMaker#dataType(Class)} with Float.class as argument.
 * <p/>
 * When order preservation is not important, use FullFloat as the data type.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FullFloat extends Number {

    private float value;

    private FullFloat() {
    }

    public FullFloat(final float value) {
        this.value = value;
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return (long) value;
    }

    @Override
    public float floatValue() {
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        else if (other == null) return false;
        else if (!(other instanceof Number)) return false;
        else return value == ((Number) other).floatValue();
    }

}
