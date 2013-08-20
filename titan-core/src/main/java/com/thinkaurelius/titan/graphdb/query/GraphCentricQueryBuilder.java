package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.thinkaurelius.titan.util.stats.ObjectAccumulator;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GraphCentricQueryBuilder implements TitanGraphQuery {

    private static final Logger log = LoggerFactory.getLogger(GraphCentricQueryBuilder.class);

    private final StandardTitanTx tx;
    private final IndexSerializer serializer;
    private List<PredicateCondition<String,TitanElement>> constraints;
    private int limit = Query.NO_LIMIT;

    public GraphCentricQueryBuilder(StandardTitanTx tx, IndexSerializer serializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx=tx;
        this.serializer = serializer;
        this.constraints = new ArrayList<PredicateCondition<String,TitanElement>>(5);
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private TitanGraphQuery has(String key, TitanPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        Preconditions.checkArgument(predicate.isValidCondition(condition),"Invalid condition: %s",condition);
        constraints.add(new PredicateCondition<String, TitanElement>(key, predicate, condition));
        return this;
    }

    @Override
    public TitanGraphQuery has(String key, com.tinkerpop.blueprints.Predicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        TitanPredicate titanPredicate = TitanPredicate.Converter.convert(predicate);
        return has(key,titanPredicate,condition);
    }

    @Override
    public TitanGraphQuery has(TitanKey key, TitanPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        return has(key.getName(),predicate,condition);
    }

    @Override
    public TitanGraphQuery has(String key) {
        return has(key,Cmp.NOT_EQUAL,(Object)null);
    }

    @Override
    public TitanGraphQuery hasNot(String key) {
        return has(key,Cmp.EQUAL,(Object)null);
    }

    @Override
    @Deprecated
    public <T extends Comparable<T>> TitanGraphQuery has(String s, T t, Compare compare) {
        return has(s,compare,t);
    }
    
    @Override
    public TitanGraphQuery has(String key, Object value) {
        return has(key,Cmp.EQUAL,value);
    }

    @Override
    public TitanGraphQuery hasNot(String key, Object value) {
        return has(key,Cmp.NOT_EQUAL,value);
    }

    @Override
    public <T extends Comparable<?>> TitanGraphQuery interval(String s, T t1, T t2) {
        has(s,Cmp.GREATER_THAN_EQUAL,t1);
        return has(s,Cmp.LESS_THAN,t2);
    }

    @Override
    public GraphCentricQueryBuilder limit(final int limit) {
        Preconditions.checkArgument(limit>=0,"Non-negative limit expected: %s",limit);
        this.limit=limit;
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    @Override
    public Iterable<Vertex> vertices() {
        GraphCentricQuery query = constructQuery(ElementType.VERTEX);
        if (QueryUtil.isEmpty(query.getCondition())) return IterablesUtil.limitedIterable(tx.getVertices(),query.getLimit());
        else return Iterables.filter(new QueryProcessor<GraphCentricQuery,TitanElement,IndexQuery>(query,tx.elementProcessor),Vertex.class);
    }

    @Override
    public Iterable<Edge> edges() {
        GraphCentricQuery query = constructQuery(ElementType.EDGE);
        if (QueryUtil.isEmpty(query.getCondition())) return IterablesUtil.limitedIterable(tx.getEdges(),query.getLimit());
        else return Iterables.filter(new QueryProcessor<GraphCentricQuery,TitanElement,IndexQuery>(query,tx.elementProcessor),Edge.class);
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private static final int DEFAULT_NO_LIMIT = 100;
    private static final int MAX_BASE_LIMIT = 20000;
    private static final int HARD_MAX_LIMIT = 50000;

    private GraphCentricQuery constructQuery(final ElementType elementType) {
        Preconditions.checkNotNull(elementType);
        if (limit==0) return GraphCentricQuery.emptyQuery();

        //Prepare constraints
        And<TitanElement> conditions = QueryUtil.constraints2QNF(tx,constraints);
        if (conditions==null) return GraphCentricQuery.emptyQuery();

        //Find most suitable index by checking all top level (i.e. AND level) predicate conditions and
        //checking which indexes cover those (we ignore NOT and nested OR since they are much less constraining).
        //For each index, count the matches. Equality constraints count more since they have much higher selectivity than all others.
        final ObjectAccumulator<String> counts = new ObjectAccumulator<String>(5);
        for (Condition<TitanElement> child : conditions.getChildren()) {
            if (child instanceof PredicateCondition) {
                PredicateCondition<TitanType,TitanElement> atom = (PredicateCondition)child;
                if (atom.getValue()!=null) {
                    Preconditions.checkArgument(atom.getKey().isPropertyKey());
                    TitanKey key = (TitanKey)atom.getKey();
                    TitanPredicate predicate = atom.getPredicate();
                    for (String index : key.getIndexes(elementType.getElementType())) {
                        if (serializer.getIndexInformation(index).supports(key.getDataType(),atom.getPredicate()))
                            counts.incBy(index,(predicate==Cmp.EQUAL)?5:1);
                    }
                }
            }
        }

        final String bestIndex = counts.getMaxObject();
        log.debug("Best index for query: {}",bestIndex);
        BackendQueryHolder<IndexQuery> query = null;
        if (bestIndex==null) {
            //No suitable index could be found => requires iterating over all elements
            query = new BackendQueryHolder<IndexQuery>(serializer.getQuery(null,null,elementType),false,null);
            query.getBackendQuery().setLimit(Query.NO_LIMIT);
        } else {
            //Filter the conditions so that matchingConditions contains all those that can be handled by bestIndex
            //and reaminingConditions contains the remaining ones. In filtering, make sure OR-clauses are considered as one
            final And<TitanElement> matchingConditions = new And<TitanElement>(conditions.size()), remainingConditions = new And<TitanElement>(conditions.size()-1);
            for (Condition<TitanElement> child : conditions.getChildren()) {
                if (isIndexCondition(bestIndex,elementType,child)) matchingConditions.add(child);
                else remainingConditions.add(child);
            }

            int indexLimit = limit==Query.NO_LIMIT?DEFAULT_NO_LIMIT:Math.min(MAX_BASE_LIMIT,limit);
            indexLimit = Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, remainingConditions, indexLimit));
            query = new BackendQueryHolder<IndexQuery>(serializer.getQuery(bestIndex,QueryUtil.simplifyQNF(matchingConditions),elementType),remainingConditions.isEmpty(),bestIndex);
            query.getBackendQuery().setLimit(indexLimit);
        }
        return new GraphCentricQuery(elementType,QueryUtil.simplifyQNF(conditions),query,limit);
    }


    private final boolean isIndexCondition(final String index,final ElementType result, Condition<TitanElement> condition) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<TitanType,TitanElement> atom = (PredicateCondition)condition;
            if (atom.getValue()!=null) {
                Preconditions.checkArgument(atom.getKey().isPropertyKey());
                TitanKey key = (TitanKey)atom.getKey();
                if (key.hasIndex(index,result.getElementType()) && serializer.getIndexInformation(index).supports(key.getDataType(),atom.getPredicate())) {
                    return true;
                } else {
                    return false;
                }
            } else return false;
        } else if (condition instanceof Not) {
            return isIndexCondition(index,result,((Not<TitanElement>)condition).getChild());
        } else if (condition instanceof Or) {
            boolean matchesAll = true;
            for (Condition<TitanElement> child : condition.getChildren()) {
                if (!isIndexCondition(index,result,child)) matchesAll=false;
            }
            return matchesAll;
        } else throw new IllegalArgumentException("Query not in QNF: " + condition);
    }

}
