package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanEdge;
import com.tinkerpop.blueprints.Direction;

/**
 * Function which gathers the state of an adjacent vertex and any properties of the connecting edge
 * into a single object.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Gather<S,M> {

    /**
     * Gathers the adjacent vertex's state and the connecting edge's properties into a single object
     * to be combined by a corresponding {@link Combiner} as configured in {@link OLAPQueryBuilder#edges(Gather, Combiner)}.
     *
     * @param state State of the adjacent vertex
     * @param edge Edge connecting to the adjacent vertex
     * @param dir Direction of the edge from the perspective of the current/central vertex
     * @return An object of type M which gathers the state and edge properties
     */
    public M apply(S state, TitanEdge edge, Direction dir);

}
