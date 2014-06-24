package com.thinkaurelius.titan.core.trigger;

import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;

import java.util.Set;

/**
 * Container interface for a set of changes against the graph. This is passed as an argument to
 * {@link ChangeProcessor#process(com.thinkaurelius.titan.core.TitanTransaction, ChangeState)}
 * for the user to retrieve changed elements.
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
    public Set<TitanVertex> getVertices(Change change);

    /**
     * Returns all relations that match the change type and any of the provided relation types. If no relation types
     * are specified all relations matching the state are returned.
     *
     * @param change
     * @param types
     * @return
     */
    public Iterable<TitanRelation> getRelations(Change change, RelationType... types);

    public Iterable<TitanEdge> getEdges(TitanVertex vertex, Change change, Direction dir, String... labels);

    public Iterable<TitanEdge> getEdges(TitanVertex vertex, Change change, Direction dir, EdgeLabel... labels);

    public Iterable<TitanProperty> getProperties(TitanVertex vertex, Change change, String... keys);

    public Iterable<TitanProperty> getProperties(TitanVertex vertex, Change change, PropertyKey... keys);


}
