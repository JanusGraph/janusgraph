package org.janusgraph.core.log;

import org.janusgraph.core.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

/**
 * Container interface for a set of changes against the graph caused by a particular transaction. This is passed as an argument to
 * {@link ChangeProcessor#process(org.janusgraph.core.JanusGraphTransaction, TransactionId, ChangeState)}
 * for the user to retrieve changed elements and act upon it.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ChangeState {

    /**
     * Returns all added, removed, or modified vertices when the change argument is {@link Change#ADDED},
     * {@link Change#REMOVED}, or {@link Change#ANY} respectively.
     *
     * @param change
     * @return
     */
    public Set<JanusGraphVertex> getVertices(Change change);

    /**
     * Returns all relations that match the change state and any of the provided relation types. If no relation types
     * are specified all relations matching the state are returned.
     *
     * @param change
     * @param types
     * @return
     */
    public Iterable<JanusGraphRelation> getRelations(Change change, RelationType... types);

    /**
     * Returns all edges incident on the given vertex in the given direction that match the provided change state and edge labels.
     *
     * @param vertex
     * @param change
     * @param dir
     * @param labels
     * @return
     */
    public Iterable<JanusGraphEdge> getEdges(Vertex vertex, Change change, Direction dir, String... labels);


    /**
     * Returns all properties incident for the given vertex that match the provided change state and property keys.
     *
     * @param vertex
     * @param change
     * @param keys
     * @return
     */
    public Iterable<JanusGraphVertexProperty> getProperties(Vertex vertex, Change change, String... keys);



}
