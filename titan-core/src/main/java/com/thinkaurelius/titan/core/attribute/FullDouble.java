package com.thinkaurelius.titan.core.attribute;

/**
 * Helper class to be used when wanting to store the full value spectrum of a double value.
 *
 * By default, Titan will convert double values into a decimal format so that it can be stored in byte-order
 * and be used for vertex-centric indices. However, that restricts the value spectrum of doubles when registered
 * using {@link com.thinkaurelius.titan.core.TypeMaker#dataType(Class)} with Double.class as argument.
 *
 * When order preservation is not important, use FullDouble as the data type.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FullDouble extends Number {

    private double value;

    private FullDouble() {}

    public FullDouble(final double value) {
        this.value=value;
    }

    @Override
    public int intValue() {
        return (int)value;
    }

    @Override
    public long longValue() {
        return (long)value;
    }

    @Override
    public float floatValue() {
        return (float)value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

}
