package com.thinkaurelius.titan.core;

/**
 * Default EdgeTypeMaker which only takes the name of the edge type to be created and uses default settings otherwise.
 *
 * @see	com.thinkaurelius.titan.core.EdgeTypeMaker
 * @see PropertyType
 * @see RelationshipType
 *
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 *
 *
 *
 */
public interface DefaultEdgeTypeMaker {

    /**
     * Creates a new relationship type with the given name and default settings against the provided EdgeTypeMaker.
     *
     * @param name Name of the relationship type
     * @param factory EdgeTypeMaker through which the relationship type is created
     * @return A new relationship type
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public RelationshipType makeRelationshipType(String name, EdgeTypeMaker factory);

    /**
     * Creates a new property type with the given name and default settings against the provided EdgeTypeMaker.
     *
     * @param name Name of the property type
     * @param factory EdgeTypeMaker through which the property type is created
     * @return A new property type
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public PropertyType makePropertyType(String name, EdgeTypeMaker factory);
}
