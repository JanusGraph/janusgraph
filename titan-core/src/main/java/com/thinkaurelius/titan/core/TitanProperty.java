
package com.thinkaurelius.titan.core;

/**
 * TitanProperty is a {@link TitanRelation} connecting a vertex to an attribute value.
 * TitanProperty extends {@link TitanRelation}, with methods for retrieving the property's attribute and key.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see TitanRelation
 * @see TitanKey
 */
public interface TitanProperty extends TitanRelation {

    /**
     * Returns the property key of this property
     *
     * @return property key of this property
     * @see TitanKey
     */
    public TitanKey getPropertyKey();


    /**
     * Returns the vertex on which this property is incident.
     *
     * @return The vertex of this property.
     */
    public TitanVertex getVertex();

    /**
     * Returns the attribute value of this property.
     *
     * @return attribute of this property
     */
    public Object getValue();

    /**
     * Returns the attribute value of this property cast to the specified class.
     *
     * @param <O>   Class to cast the attribute to
     * @param clazz Class to cast the attribute to
     * @return Attribute of this property cast to the specified class.
     * @throws ClassCastException if the attribute cannot be cast to clazz.
     */
    public <O> O getValue(Class<O> clazz);


}
