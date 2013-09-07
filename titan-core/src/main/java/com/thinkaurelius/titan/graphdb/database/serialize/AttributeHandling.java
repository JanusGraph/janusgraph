package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.TitanKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface AttributeHandling {

    public <T> void registerClass(Class<T> type, AttributeHandler<T> attributeHandler);

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

}
