package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface AttributeHandler {

    public <T> void registerClass(int registrationNo, Class<T> type, AttributeSerializer<T> attributeHandler);

    public boolean validDataType(Class datatype);

    public<V> void verifyAttribute(Class<V> datatype, Object value);

    /**
     * Converts the given (not-null) value to the this datatype V.
     * The given object will NOT be of type V.
     * Throws an {@link IllegalArgumentException} if it cannot be converted.
     *
     * @param value to convert
     * @return converted to expected datatype
     */
    public<V> V convert(Class<V> datatype, Object value);

    public boolean isOrderPreservingDatatype(Class<?> datatype);

}
