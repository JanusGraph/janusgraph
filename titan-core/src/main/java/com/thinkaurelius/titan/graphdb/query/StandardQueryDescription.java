package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.QueryDescription;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.graph.GraphCentricQuery;
import com.thinkaurelius.titan.graphdb.query.graph.JointIndexQuery;
import com.thinkaurelius.titan.graphdb.query.vertex.BaseVertexCentricQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of {@link QueryDescription}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardQueryDescription implements QueryDescription {

    private final int noCombinedQueries;
    private final Condition queryCondition;
    private final OrderList queryOrder;
    private final String toString;
    private final List<StandardSubQuery> subQueries;

    public StandardQueryDescription(int numKeys, GraphCentricQuery query) {
        this(numKeys, query.getCondition(), query.getOrder(), query, convert(query));
    }

    public StandardQueryDescription(int numVertices, BaseVertexCentricQuery query) {
        this(numVertices, query.getCondition(), query.getOrders(), query, convert(query));
    }

    private static List<StandardSubQuery> convert(GraphCentricQuery query) {
        List<StandardSubQuery> result = new ArrayList<StandardSubQuery>(query.numSubQueries());
        for (int i=0;i<query.numSubQueries();i++) {
            BackendQueryHolder<JointIndexQuery> sq = query.getSubQuery(i);
            JointIndexQuery iq = sq.getBackendQuery();
            List<String> indexes = new ArrayList<String>(iq.size());
            for (int j=0;j<iq.size();j++) indexes.add(iq.getQuery(j).getIndex().getName());
            result.add(new StandardSubQuery(sq,indexes));
        }
        return result;
    }

    private static List<StandardSubQuery> convert(BaseVertexCentricQuery query) {
        List<StandardSubQuery> result = new ArrayList<StandardSubQuery>(query.numSubQueries());
        for (int i=0;i<query.numSubQueries();i++) result.add(new StandardSubQuery(query.getSubQuery(i)));
        return result;
    }

    private StandardQueryDescription(int numCombinedQueries, Condition queryCondition, OrderList queryOrder, Query query, List<StandardSubQuery> subQueries) {
        Preconditions.checkArgument(numCombinedQueries>=1 && query!=null && subQueries!=null && queryCondition!=null && queryOrder!=null);
        this.noCombinedQueries = numCombinedQueries;
        this.queryCondition = queryCondition;
        this.queryOrder = queryOrder;
        this.toString = query.toString();
        this.subQueries = subQueries;
    }

    @Override
    public String toString() {
        return toString;
    }

    /**
     * Returns the defined order(s) of this query
     * @return
     */
    public OrderList getQueryOrder() {
        return queryOrder;
    }

    /**
     * Returns the query condition for this query
     * @return
     */
    public Condition getQueryCondition() {
        return queryCondition;
    }

    @Override
    public int getNoCombinedQueries() {
        return noCombinedQueries;
    }

    @Override
    public List<StandardSubQuery> getSubQueries() {
        return subQueries;
    }

    @Override
    public int getNoSubQueries() {
        return subQueries.size();
    }

    public static class StandardSubQuery implements SubQuery {

        private final boolean isFitted;
        private final boolean isSorted;
        private List<String> intersectingQueries;

        private StandardSubQuery(BackendQueryHolder bqh, List<String> intersectingQueries) {
            this(bqh);
            Preconditions.checkArgument(intersectingQueries!=null);
            this.intersectingQueries=intersectingQueries;
        }

        private StandardSubQuery(BackendQueryHolder bqh) {
            this.isFitted=bqh.isFitted();
            this.isSorted=bqh.isSorted();
            this.intersectingQueries = ImmutableList.of("default");
        }

        public int numIntersectingQueries() {
            return intersectingQueries.size();
        }

        public List<String> getIntersectingQueries() {
            return intersectingQueries;
        }

        @Override
        public boolean isFitted() {
            return isFitted;
        }

        @Override
        public boolean isSorted() {
            return isSorted;
        }
    }


}
