package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface ElementQuery {

    public ElementQuery and(String key, Relation relation, Object condition);

    public ElementQuery and(TitanKey key, Relation relation, Object condition);

    public Iterable<TitanVertex> getVertices();

    public Iterable<TitanEdge> getEdges();

}
