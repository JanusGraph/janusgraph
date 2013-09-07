package com.thinkaurelius.titan.core;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface AttributeHandler<V> {

    /**
     * Verifies the given (not-null) attribute value is valid.
     * Throws an {@link IllegalArgumentException} if the value is invalid,
     * otherwise simply returns.
     *
     * @param value to verify
     */
    public void verifyAttribute(V value);

    /**
     * Converts the given (not-null) value to the this datatype V.
     * The given object will NOT be of type V.
     * Throws an {@link IllegalArgumentException} if it cannot be converted.
     *
     * @param value to convert
     * @return converted to expected datatype
     */
    public V convert(Object value);


}
