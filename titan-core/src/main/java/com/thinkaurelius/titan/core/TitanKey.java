
package com.thinkaurelius.titan.core;


import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.tinkerpop.blueprints.Element;

import java.util.Collection;

/**
 * TitanKey is an extension of {@link TitanType} for properties.
 * <p/>
 * In addition to {@link TitanType}, TitanKey defines:
 * <ul>
 * <li><strong>Data Type:</strong> The accepted types of attribute values.</li>
 * <li><strong>Index:</strong> Whether attribute values are indexed. If a property key is configured to be indexed,
 * then all properties with that key are indexed which means one can retrieve vertices for that key and a value
 * via {@link TitanTransaction#getVertices(TitanKey, Object)} </li>
 * <li><strong>Uniqueness:</strong> When a TitanKey is configured to be unique it is ensured that at most one
 * vertex can be associated with a particular value for that key. As an example, <i>social security number</i>
 * is a unique property, since each SSN is associated with only one individual.</li>
 * </ul>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com)
 * @see TitanType
 */
public interface TitanKey extends TitanType {

    /**
     * Returns the data type for this property key.
     * The attributes of all properties of this type must be an instance of this data type.
     *
     * @return Data type for this property key.
     */
    public Class<?> getDataType();


    public Iterable<String> getIndexes(Class<? extends Element> elementType);


    public boolean hasIndex(String name, Class<? extends Element> elementType);


}
