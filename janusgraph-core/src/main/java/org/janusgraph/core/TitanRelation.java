
package com.thinkaurelius.titan.core;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * TitanRelation is the most abstract form of a relation between a vertex and some other entity, where
 * relation is understood in its mathematical sense. It generalizes the notion of an edge and a property.
 * <br />
 * A TitanRelation extends {@link TitanElement} which means it is an entity in its own right. This means, a TitanRelation
 * can have properties and unidirectional edges connecting it to other vertices.
 * <br />
 * A TitanRelation is an abstract concept. A TitanRelation is either a {@link TitanVertexProperty} or a {@link TitanEdge}.
 * A TitanRelation has a type which is either a label or key depending on the implementation.
 * <br />
 * A TitanRelation is either directed, or unidirected. Properties are always directed (connecting a vertex
 * with a value). A unidirected edge is a special type of directed edge where the connection is only established from the
 * perspective of the outgoing vertex. In that sense, a unidirected edge is akin to a link.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanEdge
 * @see TitanVertexProperty
 */
public interface TitanRelation extends TitanElement {

    /**
     * Retrieves the value associated with the given key on this vertex and casts it to the specified type.
     * If the key has cardinality SINGLE, then there can be at most one value and this value is returned (or null).
     * Otherwise a list of all associated values is returned, or an empty list if non exist.
     * <p/>
     *
     * @param key string identifying a key
     * @return value or list of values associated with key
     */
    public <V> V value(String key);

    /**
     * Returns the type of this relation.
     * <p/>
     * The type is either a label ({@link EdgeLabel} if this relation is an edge or a key ({@link PropertyKey}) if this
     * relation is a property.
     *
     * @return Type of this relation
     */
    public RelationType getType();

    /**
     * Returns the direction of this relation from the perspective of the specified vertex.
     *
     * @param vertex vertex on which the relation is incident
     * @return The direction of this relation from the perspective of the specified vertex.
     * @throws InvalidElementException if this relation is not incident on the vertex
     */
    public Direction direction(Vertex vertex);

    /**
     * Checks whether this relation is incident on the specified vertex.
     *
     * @param vertex vertex to check incidence for
     * @return true, if this relation is incident on the vertex, else false
     */
    public boolean isIncidentOn(Vertex vertex);

    /**
     * Checks whether this relation is a loop.
     * An relation is a loop if it connects a vertex with itself.
     *
     * @return true, if this relation is a loop, else false.
     */
    boolean isLoop();

    /**
     * Checks whether this relation is a property.
     *
     * @return true, if this relation is a property, else false.
     * @see TitanVertexProperty
     */
    boolean isProperty();

    /**
     * Checks whether this relation is an edge.
     *
     * @return true, if this relation is an edge, else false.
     * @see TitanEdge
     */
    boolean isEdge();


}
