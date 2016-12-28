
package com.thinkaurelius.titan.core;


/**
 * PropertyKey is an extension of {@link RelationType} for properties. Each property in Titan has a key.
 * <p/>
 * A property key defines the following characteristics of a property:
 * <ul>
 * <li><strong>Data Type:</strong> The data type of the value for a given property of this key</li>
 * <li><strong>Cardinality:</strong> The cardinality of the set of properties that may be associated with a single
 * vertex through a particular key.
 * </li>
 * </ul>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com)
 * @see RelationType
 */
public interface PropertyKey extends RelationType {

    /**
     * Returns the data type for this property key.
     * The values of all properties of this type must be an instance of this data type.
     *
     * @return Data type for this property key.
     */
    public Class<?> dataType();

    /**
     * The {@link Cardinality} of this property key.
     * @return
     */
    public Cardinality cardinality();

}
