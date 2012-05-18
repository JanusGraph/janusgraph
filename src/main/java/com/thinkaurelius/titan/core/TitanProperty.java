
package com.thinkaurelius.titan.core;

/**
 * A property is an edge which connects a node with an attribute.
 * In addition to an {@link TitanRelation}, a property provides method for retrieving the property's attribute.
 * 
 * The node is also called the <i>start node</i> of the property and the attribute is also called the property's <i>value</i>.
 * 
 * @see TitanRelation
 * @see TitanKey
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanProperty extends TitanRelation {

	/**
	 * Returns the property type of this property
	 * 
	 * @return property type of this property
	 */
	public TitanKey getPropertyKey();


    /**
     * Returns the vertex on which this property is incident
     *
     * @return The vertex on which this property is incident
     */
    public TitanVertex getVertex();

	/**
	 * Returns the attribute value of this property.
	 * 
	 * @return attribute of this property
	 */
	public Object getAttribute();
	
	/**
	 * Returns the attribute value of this property cast to the specified class.
	 * 
	 * @param <O> Class to cast the attribute to
	 * @param clazz Class to cast the attribute to
	 * @return Attribute of this proeprty cast to the specified class.
	 * @throws ClassCastException if the attribute cannot be cast to clazz.
	 */
	public<O> O getAttribute(Class<O> clazz);

	
}
