package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.thinkaurelius.titan.util.stats.ObjectAccumulator;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GraphCentricQueryBuilder implements TitanGraphQuery {

    private static final Logger log = LoggerFactory.getLogger(GraphCentricQueryBuilder.class);

    private final StandardTitanTx tx;
    private final IndexSerializer serializer;
    private List<PredicateCondition<String, TitanElement>> constraints;
    private OrderList orders = new OrderList();
    private int limit = Query.NO_LIMIT;

    public GraphCentricQueryBuilder(StandardTitanTx tx, IndexSerializer serializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx = tx;
        this.serializer = serializer;
        this.constraints = new ArrayList<PredicateCondition<String, TitanElement>>(5);
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private TitanGraphQuery has(String key, TitanPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        Preconditions.checkArgument(predicate.isValidCondition(condition), "Invalid condition: %s", condition);
        constraints.add(new PredicateCondition<String, TitanElement>(key, predicate, condition));
        return this;
    }

    @Override
    public TitanGraphQuery has(String key, com.tinkerpop.blueprints.Predicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        TitanPredicate titanPredicate = TitanPredicate.Converter.convert(predicate);
        return has(key, titanPredicate, condition);
    }

    @Override
    public TitanGraphQuery has(TitanKey key, TitanPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        return has(key.getName(), predicate, condition);
    }

    @Override
    public TitanGraphQuery has(String key) {
        return has(key, Cmp.NOT_EQUAL, (Object) null);
    }

    @Override
    public TitanGraphQuery hasNot(String key) {
        return has(key, Cmp.EQUAL, (Object) null);
    }

    @Override
    @Deprecated
    public <T extends Comparable<T>> TitanGraphQuery has(String s, T t, Compare compare) {
        return has(s, compare, t);
    }

    @Override
    public TitanGraphQuery has(String key, Object value) {
        return has(key, Cmp.EQUAL, value);
    }

    @Override
    public TitanGraphQuery hasNot(String key, Object value) {
        return has(key, Cmp.NOT_EQUAL, value);
    }

    @Override
    public <T extends Comparable<?>> TitanGraphQuery interval(String s, T t1, T t2) {
        has(s, Cmp.GREATER_THAN_EQUAL, t1);
        return has(s, Cmp.LESS_THAN, t2);
    }

    @Override
    public TitanGraphQuery limit(final int limit) {
        Preconditions.checkArgument(limit >= 0, "Non-negative limit expected: %s", limit);
        this.limit = limit;
        return this;
    }

    @Override
    public TitanGraphQuery orderBy(String key, Order order) {
        return orderBy(tx.getPropertyKey(key), order);
    }

    @Override
    public TitanGraphQuery orderBy(TitanKey key, Order order) {
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.getDataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.getName(), key.getDataType());
        Preconditions.checkArgument(key.isUnique(Direction.OUT), "Ordering is undefined on multi-valued key [%s]", key.getName());
        Preconditions.checkArgument(!orders.containsKey(key.getName()));
        orders.add(key, order);
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    @Override
    public Iterable<Vertex> vertices() {
        GraphCentricQuery query = constructQuery(ElementType.VERTEX);
        return Iterables.filter(new QueryProcessor<GraphCentricQuery, TitanElement, JointIndexQuery>(query, tx.elementProcessor), Vertex.class);
    }

    @Override
    public Iterable<Edge> edges() {
        GraphCentricQuery query = constructQuery(ElementType.EDGE);
        return Iterables.filter(new QueryProcessor<GraphCentricQuery, TitanElement, JointIndexQuery>(query, tx.elementProcessor), Edge.class);
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private static final int DEFAULT_NO_LIMIT = 100;
    private static final int MAX_BASE_LIMIT = 20000;
    private static final int HARD_MAX_LIMIT = 50000;

    private GraphCentricQuery constructQuery(final ElementType resultType) {
        Preconditions.checkNotNull(resultType);
        if (limit == 0) return GraphCentricQuery.emptyQuery(resultType);

        //Prepare constraints
        And<TitanElement> conditions = QueryUtil.constraints2QNF(tx, constraints);
        if (conditions == null) return GraphCentricQuery.emptyQuery(resultType);

        orders.makeImmutable();
        if (orders.isEmpty()) orders = OrderList.NO_ORDER;

        //Count how many conditions are not covered by an index
        int andClausesNotCovered = 0;
        Map<Condition, Set<String>> andConditionCoverage = Maps.newHashMap();
        for (Condition child : conditions.getChildren()) {
            Set<String> indexes = QueryUtil.andClauseIndexCover(resultType, child, serializer);
            if (!indexes.isEmpty()) {
                andConditionCoverage.put(child, indexes);
            } else {
                andClausesNotCovered++;
            }
        }

        BackendQueryHolder<JointIndexQuery> query;
        if (!andConditionCoverage.isEmpty()) {
            JointIndexQuery jointQuery = new JointIndexQuery();
            boolean isSorted = true;

            while (!andConditionCoverage.isEmpty()) {
                final ObjectAccumulator<String> counts = new ObjectAccumulator<String>(5);
                for (Set<String> indexes : andConditionCoverage.values()) {
                    for (String index : indexes) counts.incBy(index, 1.0);
                }
                //Give extra credit to indexes that cover the order
                for (String index : counts.getObjects()) {
                    if (indexCoversOrder(index, orders, resultType)) counts.incBy(index,1.0);
                }
                final String bestIndex = counts.getMaxObject();
                Preconditions.checkNotNull(bestIndex);
                boolean supportsOrder = indexCoversOrder(bestIndex, orders, resultType);

                final And<TitanElement> matchingCond = new And<TitanElement>((int) counts.getCount(bestIndex));
                Iterator<Map.Entry<Condition, Set<String>>> conditer = andConditionCoverage.entrySet().iterator();
                while (conditer.hasNext()) {
                    Map.Entry<Condition, Set<String>> entry = conditer.next();
                    if (entry.getValue().contains(bestIndex)) {
                        matchingCond.add(entry.getKey());
                        conditer.remove();
                    }
                }
                final IndexQuery subquery = serializer.getQuery(bestIndex, resultType, matchingCond, supportsOrder ? orders : OrderList.NO_ORDER);
                jointQuery.add(bestIndex, subquery);
                isSorted = isSorted && supportsOrder;
            }

            /* TODO: smarter optimization:
            1) use only one PredicateCondition if in-unique
            2) use in-memory histograms to estimate selectivity of PredicateConditions and filter out low-selectivity ones if they would result in an individual index call (better to filter afterwards in memory)
            */

            int indexLimit = limit == Query.NO_LIMIT ? DEFAULT_NO_LIMIT : Math.min(MAX_BASE_LIMIT, limit);
            indexLimit = Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, andClausesNotCovered, indexLimit));
            jointQuery.setLimit(indexLimit);
            query = new BackendQueryHolder<JointIndexQuery>(jointQuery, andClausesNotCovered == 0, isSorted, null);
        } else {
            query = new BackendQueryHolder<JointIndexQuery>(new JointIndexQuery(), false, false, null);
        }

        return new GraphCentricQuery(resultType, conditions, orders, query, limit);
    }

    private static final boolean indexCoversOrder(String index, OrderList orders, ElementType resultType) {
        if (orders.isEmpty()) return true;
        else if (index.equals(Titan.Token.STANDARD_INDEX)) return false;
        for (int i = 0; i < orders.size(); i++) {
            boolean found = false;
            for (String keyindex : orders.getKey(i).getIndexes(resultType.getElementType())) {
                if (keyindex.equals(index)) found = true;
            }
            if (!found) return false;
        }
        return true;
    }


}
