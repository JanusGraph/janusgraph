package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
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
        return Iterables.filter(new QueryProcessor<GraphCentricQuery,TitanElement,IndexQuery>(query,tx.elementProcessor),Vertex.class);
    }

    @Override
    public Iterable<Edge> edges() {
        GraphCentricQuery query = constructQuery(ElementType.EDGE);
        return Iterables.filter(new QueryProcessor<GraphCentricQuery,TitanElement,IndexQuery>(query,tx.elementProcessor),Edge.class);
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
        And<TitanElement> conditions = new And<TitanElement>(constraints.size()+4);
        if (!QueryUtil.prepareConstraints(tx,conditions,constraints)) return GraphCentricQuery.emptyQuery();

        //Find most suitable index
        final ObjectAccumulator<String> counts = new ObjectAccumulator<String>(5);
        for (Condition<TitanElement> child : conditions.getChildren()) {
            if (child instanceof PredicateCondition) {
                PredicateCondition<TitanType,TitanElement> atom = (PredicateCondition)child;
                if (atom.getCondition()!=null) {
                    Preconditions.checkArgument(atom.getKey().isPropertyKey());
                    TitanKey key = (TitanKey)atom.getKey();
                    TitanPredicate predicate = atom.getPredicate();
                    for (String index : key.getIndexes(elementType.getElementType())) {
                        if (serializer.getIndexInformation(index).supports(key.getDataType(),atom.getPredicate()))
                            counts.incBy(index,(predicate==Cmp.EQUAL || predicate== Contain.IN)?2:1);
                    }
                }
            }
        }

        final String bestIndex = counts.getMaxObject();
        log.debug("Best index for query: {}",bestIndex);
        BackendQueryHolder<IndexQuery> query = null;
        if (bestIndex==null) {
            //No suitable index could be found
            query = new BackendQueryHolder<IndexQuery>(serializer.getQuery(null,null,elementType),false,null);
            query.getBackendQuery().setLimit(Query.NO_LIMIT);
        } else {
            //Filter out IndexQuery
            final And<TitanElement> matchingConditions = new And<TitanElement>(conditions.size()), remainingConditions = new And<TitanElement>(conditions.size()-1);
            for (Condition<TitanElement> child : conditions.getChildren()) {
                if (child instanceof PredicateCondition) {
                    PredicateCondition<TitanType,TitanElement> atom = (PredicateCondition)child;
                    if (atom.getCondition()!=null) {
                        Preconditions.checkArgument(atom.getKey().isPropertyKey());
                        TitanKey key = (TitanKey)atom.getKey();
                        if (serializer.getIndexInformation(bestIndex).supports(key.getDataType(),atom.getPredicate())) {
                            matchingConditions.add(child);
                        } else {
                            remainingConditions.add(child);
                        }
                    }
                }
            }
            int indexLimit = limit==Query.NO_LIMIT?DEFAULT_NO_LIMIT:Math.min(MAX_BASE_LIMIT,limit);
            indexLimit = Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, remainingConditions, indexLimit));
            query = new BackendQueryHolder<IndexQuery>(serializer.getQuery(bestIndex,matchingConditions,elementType),remainingConditions.isEmpty(),bestIndex);
            query.getBackendQuery().setLimit(indexLimit);
        }
        return new GraphCentricQuery(elementType,conditions,query,limit);
    }


}
