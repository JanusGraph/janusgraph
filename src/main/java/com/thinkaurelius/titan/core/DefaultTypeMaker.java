package com.thinkaurelius.titan.core;

/**
 * Default TypeMaker which only takes the name of the edge type to be created and uses default settings otherwise.
 *
 * @see    TypeMaker
 * @see TitanKey
 * @see TitanLabel
 *
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 *
 *
 *
 */
public interface DefaultTypeMaker {

    /**
     * Creates a new relationship type with the given name and default settings against the provided TypeMaker.
     *
     * @param name Name of the relationship type
     * @param factory TypeMaker through which the relationship type is created
     * @return A new relationship type
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public TitanLabel makeRelationshipType(String name, TypeMaker factory);

    /**
     * Creates a new property type with the given name and default settings against the provided TypeMaker.
     *
     * @param name Name of the property type
     * @param factory TypeMaker through which the property type is created
     * @return A new property type
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public TitanKey makePropertyType(String name, TypeMaker factory);
}
