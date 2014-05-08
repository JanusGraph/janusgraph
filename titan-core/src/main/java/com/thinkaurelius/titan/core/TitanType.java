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
public interface TitanType extends TitanVertex, TitanSchemaElement, Namifiable {

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
