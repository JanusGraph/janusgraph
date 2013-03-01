package com.thinkaurelius.titan.graphdb.query;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.tinkerpop.blueprints.Direction;

public class QueryUtil {

    public static final TitanProperty queryHiddenUniqueProperty(InternalVertex vertex, TitanKey key) {
        assert ((InternalType) key).isHidden() : "Expected hidden property key";
        assert key.isUnique(Direction.OUT) : "Expected functional property  type";
        return Iterables.getOnlyElement(
                new VertexCentricQueryBuilder(vertex).
                        includeHidden().
                        type(key).
                        properties(), null);
    }

    public static final Iterable<TitanRelation> queryAll(InternalVertex vertex) {
        return new VertexCentricQueryBuilder(vertex).includeHidden().relations();
    }

    public static final int updateLimit(int limit, double multiplier) {
        return (int)Math.min(Integer.MAX_VALUE-1,Math.round(limit*multiplier));
    }

}
