// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.*;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.*;
import org.janusgraph.graphdb.util.CloseableIteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;

/**
 * Builds a {@link JanusGraphQuery}, optimizes the query and compiles the result into a {@link GraphCentricQuery} which
 * is then executed through a {@link QueryProcessor}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class GraphCentricQueryBuilder implements JanusGraphQuery<GraphCentricQueryBuilder> {

    private static final Logger log = LoggerFactory.getLogger(GraphCentricQueryBuilder.class);
    /**
     * Transaction in which this query is executed.
     */
    private final StandardJanusGraphTx tx;
    /**
     * Serializer used to serialize the query conditions into backend queries.
     */
    private final IndexSerializer serializer;
    /**
     * The constraints added to this query. None by default.
     */
    private final List<PredicateCondition<String, JanusGraphElement>> constraints = new ArrayList<>(5);

    /**
     * List of constraints added to an Or query. None by default
     */
    private final List<List<PredicateCondition<String, JanusGraphElement>>> globalConstraints = new ArrayList<>();
    /**
     * The order in which the elements should be returned. None by default.
     */
    private OrderList orders = new OrderList();
    /**
     * The limit of this query. No limit by default.
     */
    private int limit = Query.NO_LIMIT;
    /**
     * The profiler observing this query
     */
    private QueryProfiler profiler = QueryProfiler.NO_OP;
    /**
     * Whether smart limit adjustment is enabled
     */
    private boolean useSmartLimit;
    /**
     * Selection service for the best combination of indexes to be queried
     */
    private IndexSelectionStrategy indexSelector;

    public GraphCentricQueryBuilder(StandardJanusGraphTx tx, IndexSerializer serializer, IndexSelectionStrategy indexSelector) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        useSmartLimit = tx.getGraph().getConfiguration().adjustQueryLimit();
        this.tx = tx;
        this.serializer = serializer;
        this.indexSelector = indexSelector;
    }

    public void disableSmartLimit() {
        useSmartLimit = false;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    public List<PredicateCondition<String, JanusGraphElement>> getConstraints() {
        return constraints;
    }

    public GraphCentricQueryBuilder profiler(QueryProfiler profiler) {
        Preconditions.checkNotNull(profiler);
        this.profiler=profiler;
        return this;
    }

    @Override
    public GraphCentricQueryBuilder has(String key, JanusGraphPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        Preconditions.checkArgument(predicate.isValidCondition(condition),
                "Invalid condition: %s", condition);
        if (predicate.equals(Contain.NOT_IN)) {
            // when querying `has(key, without(value))`, the query must also satisfy `has(key)`
            has(key);
        }
        constraints.add(new PredicateCondition<>(key, predicate, condition));
        return this;
    }

    public GraphCentricQueryBuilder has(PropertyKey key, JanusGraphPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        return has(key.name(),predicate,condition);
    }

    @Override
    public GraphCentricQueryBuilder has(String key) {
        return has(key, Cmp.NOT_EQUAL, null);
    }

    @Override
    public GraphCentricQueryBuilder hasNot(String key) {
        return has(key, Cmp.EQUAL, null);
    }

    @Override
    public GraphCentricQueryBuilder has(String key, Object value) {
        return has(key, Cmp.EQUAL, value);
    }

    @Override
    public GraphCentricQueryBuilder hasNot(String key, Object value) {
        return has(key, Cmp.NOT_EQUAL, value);
    }

    @Override
    public <T extends Comparable<?>> GraphCentricQueryBuilder interval(String s, T t1, T t2) {
        has(s, Cmp.GREATER_THAN_EQUAL, t1);
        return has(s, Cmp.LESS_THAN, t2);
    }

    @Override
    public GraphCentricQueryBuilder limit(final int limit) {
        Preconditions.checkArgument(limit >= 0, "Non-negative limit expected: %s", limit);
        this.limit = limit;
        return this;
    }

    @Override
    public GraphCentricQueryBuilder orderBy(String keyName,  org.apache.tinkerpop.gremlin.process.traversal.Order order) {
        Preconditions.checkArgument(tx.containsPropertyKey(keyName),"Provided key does not exist: %s",keyName);
        final PropertyKey key = tx.getPropertyKey(keyName);
        Preconditions.checkArgument(key!=null && order!=null,"Need to specify and key and an order");
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.dataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.name(), key.dataType());
        Preconditions.checkArgument(key.cardinality()== Cardinality.SINGLE,
                "Ordering is undefined on multi-valued key [%s]", key.name());
        Preconditions.checkArgument(!orders.containsKey(key));
        orders.add(key, Order.convert(order));
        return this;
    }

    @Override
    public GraphCentricQueryBuilder or(GraphCentricQueryBuilder subQuery) {
        this.globalConstraints.add(subQuery.getConstraints());
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    @Override
    public Iterable<JanusGraphVertex> vertices() {
        return iterables(constructQuery(ElementCategory.VERTEX), JanusGraphVertex.class);
    }

    @Override
    public Iterable<JanusGraphEdge> edges() {
        return iterables(constructQuery(ElementCategory.EDGE), JanusGraphEdge.class);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> properties() {
        return iterables(constructQuery(ElementCategory.PROPERTY), JanusGraphVertexProperty.class);
    }

    public <E extends JanusGraphElement> Iterable<E> iterables(final GraphCentricQuery query, final Class<E> aClass) {
        return new Iterable<E>() {
            @Override
            public CloseableIterator<E> iterator() {
                return CloseableIteratorUtils.filter(new QueryProcessor(query, tx.elementProcessor).iterator(),
                    (Predicate<? super E>) e -> aClass.isInstance(e));
            }

        };
    }


    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private static final int DEFAULT_NO_LIMIT = 1000;
    private static final int MAX_BASE_LIMIT = 20000;
    private static final int HARD_MAX_LIMIT = 100000;

    public GraphCentricQuery constructQuery(final ElementCategory resultType) {
        final QueryProfiler optProfiler = profiler.addNested(QueryProfiler.OPTIMIZATION);
        optProfiler.startTimer();
        if (this.globalConstraints.isEmpty()) {
            this.globalConstraints.add(this.constraints);
        }
        final GraphCentricQuery query = constructQueryWithoutProfile(resultType);
        optProfiler.stopTimer();
        query.observeWith(profiler);
        return query;
    }

    public GraphCentricQuery constructQueryWithoutProfile(final ElementCategory resultType) {
        Preconditions.checkNotNull(resultType);
        if (limit == 0) return GraphCentricQuery.emptyQuery(resultType);

        //Prepare constraints
        final MultiCondition<JanusGraphElement> conditions;
        if (this.globalConstraints.size() == 1) {
            conditions = QueryUtil.constraints2QNF(tx, constraints);
            if (conditions == null) return GraphCentricQuery.emptyQuery(resultType);
        } else {
            conditions = new Or<>();
            for (final List<PredicateCondition<String, JanusGraphElement>> child : this.globalConstraints){
                final And<JanusGraphElement> localconditions = QueryUtil.constraints2QNF(tx, child);
                if (localconditions == null) return GraphCentricQuery.emptyQuery(resultType);
                conditions.add(localconditions);
            }
        }

        //Prepare orders
        orders.makeImmutable();
        if (orders.isEmpty()) orders = OrderList.NO_ORDER;

        //Compile all indexes that cover at least one of the query conditions
        final Set<IndexType> indexCandidates = IndexSelectionUtil.getMatchingIndexes(conditions);

        indexCandidates.removeIf(
            indexType -> (indexType.getElement() != resultType)
                    || (conditions instanceof Or
                    && (indexType.isCompositeIndex() || !serializer.features((MixedIndexType) indexType).supportNotQueryNormalForm())));

        final Set<Condition> coveredClauses = new HashSet<>();
        final IndexSelectionStrategy.SelectedIndexQuery selectedIndex = indexSelector.selectIndices(indexCandidates, conditions, coveredClauses, orders, serializer);

        BackendQueryHolder<JointIndexQuery> query;
        if (!coveredClauses.isEmpty()) {
            int indexLimit = limit == Query.NO_LIMIT ? HARD_MAX_LIMIT : limit;
            if (useSmartLimit) {
                indexLimit = limit == Query.NO_LIMIT ? DEFAULT_NO_LIMIT : Math.min(MAX_BASE_LIMIT, limit);
            }
            indexLimit = Math.min(HARD_MAX_LIMIT,
                QueryUtil.adjustLimitForTxModifications(tx, conditions.numChildren() - coveredClauses.size(), indexLimit));
            query = new BackendQueryHolder<>(selectedIndex.getQuery().updateLimit(indexLimit),
                    coveredClauses.size() == conditions.numChildren(), selectedIndex.isSorted());
        } else {
            query = new BackendQueryHolder<>(new JointIndexQuery(), false, selectedIndex.isSorted());
        }
        return new GraphCentricQuery(resultType, conditions, orders, query, limit);
    }
}
