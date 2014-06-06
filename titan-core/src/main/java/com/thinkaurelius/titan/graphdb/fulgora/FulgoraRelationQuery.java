package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.olap.Combiner;
import com.thinkaurelius.titan.core.olap.Gather;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.List;

/**
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
*/
class FulgoraRelationQuery<M> {

    final List<SliceQuery> queries;
    final Combiner<M> combiner;

    FulgoraRelationQuery(List<SliceQuery> queries, Combiner<M> combiner) {
        Preconditions.checkArgument(queries !=null && !queries.isEmpty() && combiner!=null);
        this.queries = queries;
        this.combiner = combiner;
    }
}


class FulgoraPropertyQuery<M> extends FulgoraRelationQuery<M> {

    final Function<TitanProperty,M> gather;

    FulgoraPropertyQuery(List<SliceQuery> query, Function<TitanProperty, M> gather, Combiner<M> combiner) {
        super(query, combiner);
        this.gather = gather;
    }

    M process(TitanProperty p, @Nullable M previous) {
        M result = gather.apply(p);
        if (previous==null) return result;
        else return combiner.combine(previous,result);
    }

    public static final Function<TitanProperty,Object> SINGLE_VALUE_GATHER = new Function<TitanProperty, Object>() {
        @Override
        public Object apply(TitanProperty titanProperty) {
            return titanProperty.getValue();
        }
    };

    public static final Function<TitanProperty,List<Object>> VALUE_LIST_GATHER = new Function<TitanProperty, List<Object>>() {
        @Override
        public List<Object> apply(TitanProperty titanProperty) {
            return Lists.newArrayList(titanProperty.getValue());
        }
    };

    public static final Combiner<Object> SINGLE_COMBINER = new Combiner<Object>() {
        @Override
        public Object combine(Object m1, Object m2) {
            throw new IllegalStateException("Expected at most one value but got multiple");
        }
    };

    public static final Combiner<List<Object>> VALUE_LIST_COMBINER = new Combiner<List<Object>>() {
        @Override
        public List<Object> combine(List<Object> m1, List<Object> m2) {
            m1.addAll(m2);
            return m1;
        }
    };

}

class FulgoraEdgeQuery<S,M> extends FulgoraRelationQuery<M> {

    final Gather<S,M> gather;

    FulgoraEdgeQuery(List<SliceQuery> query, Gather<S, M> gather, Combiner<M> combiner) {
        super(query, combiner);
        this.gather = gather;
    }

    M process(TitanEdge e, Direction dir, S state, @Nullable M previous) {
        M result = gather.apply(state,e, dir);
        if (previous==null) return result;
        else return combiner.combine(previous,result);
    }

    public static final Gather DEFAULT_GATHER = new Gather() {
        @Override
        public Object apply(Object state, TitanEdge edge, Direction dir) {
            return state;
        }
    };

    public static final<S> Gather<S,S> getDefaultGather() {
        return DEFAULT_GATHER;
    }

}
