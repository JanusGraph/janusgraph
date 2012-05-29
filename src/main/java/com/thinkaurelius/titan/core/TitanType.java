
package com.thinkaurelius.titan.core;


/**
 * TitanType defines the schema for {@link TitanRelation}. TitanType can be configured through {@link TypeMaker} to
 * provide data verification, better storage efficiency, and higher retrieval performance.
 * <br />
 * Each {@link TitanRelation} has a unique type which defines many important characteristics of that relation.
 * <ul>
 *     <li><strong>Functional:</strong> Defines relations of this type to be functional ({@link #isFunctional()}</li>
 *     <li><strong>Simple:</strong> Defines relations of this type to NOT have incident properties which provides better storage efficiencies in case properties are not needed. ({@link #isSimple()}</li>
 *     <li><strong>Groups:</strong> Types can be grouped together for faster retrieval ({@link #getGroup()}</li>
 * </ul>
 * <br />
 * TitanTypes are constructed through {@link TypeMaker} which is accessed in the context of a {@link TitanTransaction}
 * via {@link com.thinkaurelius.titan.core.TitanTransaction#makeType()}.
 * <br />
 * TitanType names must be unique in a graph database. Many methods allow the name of the type as an argument
 * instead of the actual type reference.
 * 
 * @see TitanRelation
 * @see TypeMaker
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-configuration">Titan Type Wiki</a>
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 *
 */
public interface TitanType extends TitanVertex {

	/**
	 * Returns the unique name of this type.
     *
	 * @return Name of this type.
	 */
	public String getName();
	
	/**
	 * Checks whether this type is functional.
     *
	 * A type is functional, if its relation instances are functional in the mathematical sense.
	 * A relation is functional iff each object in the domain has at most one value.
     * Specifically, this means:
     * <ul>
     *  <li>A property key is functional, if a vertex has at most one value associated with the key.
     *  <i>Social security number</i> is an example of a functional property key since each person has at most SSN.</li>
     *  <li>An edge lable is functional, if a vertex has at most one outgoing edge for that label.
     *  <i>Father</i> is an exmaple of a functional edge label, since each person has at most one father.</li>
     * </ul>
	 * 
	 * @return true, if the type is functional, else false
	 */
	public boolean isFunctional();
	
	/**
	 * Checks whether relations of this type are modifiable after creation.
     *
     * TitanType can be designated <i>non-modifiable</i> which means all of its relation instances cannot be modified
     * after creation.
	 * 
	 * @return true, if relations of this type are modifiable, else false.
	 */
	public boolean isModifiable();

    /**
     * Checks whether relations of this type are simple.
     *
     * If a TitanType is simple, all of its relations are defined to be simple, which means that they do not accept
     * incident properties. Simple relations are more storage efficient.
     *
     * @return true, if relations of this type are simple, else false.
     */
	public boolean isSimple();

	/**
	 * Returns the type group of this type.
     *
     * TitanTypes can be grouped together which makes it more efficient to retrieve all relations associated with the
     * type group.
	 * 
	 * @return type group of this type.
	 * @see TypeGroup
	 */
	public TypeGroup getGroup();
	
	/**
	 * Checks if this type is a property key
	 * 
	 * @return true, if this type is a property key, else false.
	 * @see TitanKey
	 */
	public boolean isPropertyKey();
	
	/**
	 * Checks if this type is an edge label
	 * 
	 * @return true, if this type is a edge label, else false.
	 * @see TitanLabel
	 */
	public boolean isEdgeLabel();
	
}
