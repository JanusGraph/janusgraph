package com.thinkaurelius.titan.core;


import com.tinkerpop.blueprints.Direction;

/**
 * TitanType defines the schema for {@link TitanRelation}. TitanType can be configured through {@link TypeMaker} to
 * provide data verification, better storage efficiency, and higher retrieval performance.
 * <br />
 * Each {@link TitanRelation} has a unique type which defines many important characteristics of that relation.
 * <ul>
 * <li><strong>Uniqueness:</strong> Defines relations of this type to be unique in the outgoing or incoming direction ({@link #isUnique(com.tinkerpop.blueprints.Direction)}}.
 * If a type is unique, that means there can be at most one relation (i.e. property or edge) of that type on any given element in that direction.</li>
 * </ul>
 * <br />
 * TitanTypes are constructed through {@link TypeMaker} which is accessed in the context of a {@link TitanTransaction}
 * via {@link com.thinkaurelius.titan.core.TitanTransaction#makeKey(String)} for property keys or {@link com.thinkaurelius.titan.core.TitanTransaction#makeLabel(String)}
 * for edge labels. Identical methods exist on {@link TitanGraph}.
 * Note, types will only be visible once the transaction in which they were created has been committed.
 * <br />
 * TitanType names must be unique in a graph database. Many methods allow the name of the type as an argument
 * instead of the actual type reference.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see TitanRelation
 * @see TypeMaker
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-configuration">Titan Type Wiki</a>
 */
public interface TitanType extends TitanVertex {

    /**
     * Returns the unique name of this type.
     *
     * @return Name of this type.
     */
    public String getName();

    /**
     * Checks whether this unique in the given direction.
     * <p/>
     * A type is unique, iff there can be at most one relation (i.e. property or edge) of that type on any given element in that direction.
     * We say that a type is out-unique (in-unique) if it is unique in the OUT direction (resp. IN direction).
     * <p/>
     * Specifically, this means:
     * <ul>
     * <li>A property key is out-unique, if a vertex has at most one value associated with the key. This is called single valued in contrast to list-valued.
     * <i>Birthdate</i> is an example of an out-unique property key since each person has at most one birth date.</li>
     * <li>A property key is in-unique, if at most one vertex can be assigned a given value. This is called unique across the graph.
     * <i>Social security number</i> is an example of an in-unique property key since there can only be one person for a particular SSN.</li>
     * <li>An edge lable is out-unique, if a vertex has at most one outgoing edge for that label. For edges, out- and in-uniqueness translate to cardinality constraints.
     * <i>Father</i> is an exmaple of a functional edge label, since each person has at most one father.</li>
     * </ul>
     *
     * @return true, if the type is unique in the given direction, else false
     */
    public boolean isUnique(Direction direction);

    /**
     * Checks whether relations of this type are modifiable after creation.
     * <p/>
     * TitanType can be designated <i>non-modifiable</i> which means all of its relation instances cannot be modified
     * after creation.
     *
     * @return true, if relations of this type are modifiable, else false.
     */
    public boolean isModifiable();

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
