
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;

/**
 * TitanRelation is the most abstract form of a relation between a vertex and some other entity, where
 * relation is understood in its mathematical sense. It generalizes the notion of an edge and a property.
 * <br />
 * A TitanRelation extends {@link TitanVertex} which means it is an entity in its own right. This means, a TitanRelation
 * can have properties and unidirectional edges connecting it to other vertices.
 * <br />
 * A TitanRelation is an abstract concept. A TitanRelation is either a {@link TitanProperty} or a {@link TitanEdge}.
 * A TitanRelation has a type which is either a label or key depending on the implementation.
 * <br />
 * A TitanRelation is either directed, or unidirected. Properties are always directed (connecting a vertex
 * with an attribute). A unidirected edge is a special type of directed edge where the connection is only established from the
 * perspective of the outgoing vertex. In that sense, a unidirected edge is akin to a link.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanEdge
 * @see TitanProperty
 */
public interface TitanRelation extends TitanElement {

    /**
     * Establishes a unidirectional edge between this relation and the given vertex for the specified label.
     * The label must be defined {@link com.thinkaurelius.titan.core.TitanLabel#isUnidirected()}.
     *
     * @param label
     * @param vertex
     */
    public void setProperty(TitanLabel label, TitanVertex vertex);

    /**
     * Returns the vertex associated to this relation by a unidirected edge of the given label or NULL if such does not exist.
     *
     * @param label
     * @return
     */
    public TitanVertex getProperty(TitanLabel label);

    /**
     * Returns the type of this relation.
     * <p/>
     * The type is either a label ({@link TitanLabel} if this relation is an edge or a key ({@link TitanKey}) if this
     * relation is a property.
     *
     * @return Type of this relation
     */
    public TitanType getType();

    /**
     * Returns the direction of this relation from the perspective of the specified vertex.
     *
     * @param vertex vertex on which the relation is incident
     * @return The direction of this relation from the perspective of the specified vertex.
     * @throws InvalidElementException if this relation is not incident on the vertex
     */
    public Direction getDirection(TitanVertex vertex);

    /**
     * Checks whether this relation is incident on the specified vertex.
     *
     * @param vertex vertex to check incidence for
     * @return true, if this relation is incident on the vertex, else false
     */
    public boolean isIncidentOn(TitanVertex vertex);

    /**
     * Checks whether this relation can be modified in the context of this transaction.
     *
     * @return true, if this relation can be modified, else false.
     */
    public boolean isModifiable();

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
     * @see TitanProperty
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
