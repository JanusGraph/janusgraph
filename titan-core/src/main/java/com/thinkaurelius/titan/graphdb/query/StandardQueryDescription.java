package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.QueryDescription;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.graph.GraphCentricQuery;
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
        this(numKeys, query.getCondition(), query.getOrder(), query);
    }

    public StandardQueryDescription(int numVertices, BaseVertexCentricQuery query) {
        this(numVertices, query.getCondition(), query.getOrders(), query, convert(query));
    }

    private StandardQueryDescription(int numCombinedQueries, Condition queryCondition, OrderList queryOrder, ElementQuery query) {
        this(numCombinedQueries,queryCondition,queryOrder,query,convert(query));
    }

    private static List<BackendQueryHolder<?>> convert(ElementQuery query) {
        List<BackendQueryHolder<?>> result = new ArrayList<BackendQueryHolder<?>>(query.numSubQueries());
        for (int i=0;i<query.numSubQueries();i++) result.add(query.getSubQuery(i));
        return result;
    }

    private static List<BackendQueryHolder<?>> convert(BaseVertexCentricQuery query) {
        List<BackendQueryHolder<?>> result = new ArrayList<BackendQueryHolder<?>>(query.numSubQueries());
        for (int i=0;i<query.numSubQueries();i++) result.add(query.getSubQuery(i));
        return result;
    }

    private StandardQueryDescription(int numCombinedQueries, Condition queryCondition, OrderList queryOrder, Query query, List<BackendQueryHolder<?>> subQueries) {
        Preconditions.checkArgument(numCombinedQueries>=1 && query!=null && subQueries!=null && queryCondition!=null && queryOrder!=null);
        this.noCombinedQueries = numCombinedQueries;
        this.queryCondition = queryCondition;
        this.queryOrder = queryOrder;
        this.toString = query.toString();
        this.subQueries = new ArrayList<StandardSubQuery>(subQueries.size());
        for (BackendQueryHolder bqh : subQueries) {
            this.subQueries.add(new StandardSubQuery(bqh));
        }
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

    private class StandardSubQuery implements SubQuery {

        private final boolean isFitted;
        private final boolean isSorted;

        private StandardSubQuery(BackendQueryHolder<?> bqh) {
            this.isFitted=bqh.isFitted();
            this.isSorted=bqh.isSorted();
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
