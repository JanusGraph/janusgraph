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
import com.google.common.collect.ImmutableList;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.QueryProcessor;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.util.CloseableIteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ADJUST_LIMIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.HARD_MAX_LIMIT;

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
     * List of constraints added to one or more Or queries. If there are multiple they will be added to an And query.
     * None by default.
     */
    private final List<List<List<PredicateCondition<String, JanusGraphElement>>>> globalConstraints = new ArrayList<>();
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
     * The hard max limit of each query
     */
    private int hardMaxLimit;
    /**
     * Selection service for the best combination of indexes to be queried
     */
    private IndexSelectionStrategy indexSelector;
    /**
     * Whether the elements without a value of an order key are left out of the result, rather than coming last
     */
    private boolean requiresOrderValues = false;

    public GraphCentricQueryBuilder(StandardJanusGraphTx tx, IndexSerializer serializer, IndexSelectionStrategy indexSelector) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        Configuration customOptions = tx.getConfiguration().getCustomOptions();
        GraphDatabaseConfiguration graphConfigs = tx.getGraph().getConfiguration();
        useSmartLimit = customOptions.has(ADJUST_LIMIT) ? customOptions.get(ADJUST_LIMIT) : graphConfigs.adjustQueryLimit();
        hardMaxLimit = customOptions.has(HARD_MAX_LIMIT) ? customOptions.get(HARD_MAX_LIMIT) : graphConfigs.getHardMaxLimit();
        this.tx = tx;
        this.serializer = serializer;
        this.indexSelector = indexSelector;
    }

    public void disableSmartLimit() {
        useSmartLimit = false;
    }

    /**
     * Leaves the elements without a value of an order key out of the result, as TinkerPop's {@code order().by(key)}
     * does, rather than placing them last. Unlike a condition of {@link #has(String)}, this one doesn't take part in
     * choosing the indexes which answer the query, so that an index which holds the key isn't read for every element
     * which has a value of it: a mixed index which the query reads for its other conditions checks it where it holds
     * the key, and the query checks it in memory otherwise. An order of vertices by a key of several values, by which
     * only an index can order them, keeps the vertices without a value of any of its keys, last, as the vertices which
     * the transaction changed can't be ordered by such a key in memory; an edge holds one value of a key at most.
     *
     * @return this query builder
     */
    public GraphCentricQueryBuilder requireOrderValues() {
        requiresOrderValues = true;
        return this;
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
        if (key == null) {
            return has((String) null, predicate, condition);
        }
        return has(key.name(), predicate, condition);
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
        //Preconditions.checkArgument(key.cardinality()== Cardinality.SINGLE,
        //        "Ordering is undefined on multi-valued key [%s]", key.name());
        Preconditions.checkArgument(!orders.containsKey(key), "orders [%s] already contains key [%s]", orders, key);
        orders.add(key, Order.convert(order));
        return this;
    }

    @Override
    public GraphCentricQueryBuilder or(Collection<GraphCentricQueryBuilder> subQueries) {
        final List<List<PredicateCondition<String, JanusGraphElement>>> constraints = new ArrayList<>(subQueries.size());
        subQueries.forEach(subQuery -> constraints.add(subQuery.getConstraints()));
        this.globalConstraints.add(constraints);
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
        return () -> CloseableIteratorUtils.filter(new QueryProcessor(query, tx.elementProcessor).iterator(),
            (Predicate<? super E>) aClass::isInstance);
    }


    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private static final int DEFAULT_NO_LIMIT = 1000;
    private static final int MAX_BASE_LIMIT = 20000;

    public GraphCentricQuery constructQuery(final ElementCategory resultType) {
        final QueryProfiler optProfiler = profiler.addNested(QueryProfiler.OPTIMIZATION);
        optProfiler.startTimer();
        final GraphCentricQuery query = constructQueryWithoutProfile(resultType);
        optProfiler.stopTimer();
        query.observeWith(profiler);
        return query;
    }

    public GraphCentricQuery constructQueryWithoutProfile(final ElementCategory resultType) {
        Preconditions.checkNotNull(resultType);
        if (limit == 0) return GraphCentricQuery.emptyQuery(resultType);

        if (globalConstraints.isEmpty()) {
            globalConstraints.add(Collections.singletonList(constraints));
        }
        //Prepare constraints
        final MultiCondition<JanusGraphElement> conditions;
        if (this.globalConstraints.size() == 1 && this.globalConstraints.get(0).size() == 1) {
            conditions = QueryUtil.constraints2QNF(tx, globalConstraints.get(0).get(0));
            if (conditions == null) return GraphCentricQuery.emptyQuery(resultType);
        } else {
            if (this.globalConstraints.size() == 1) {
                conditions = constructOrCondition(this.globalConstraints.get(0));
                if (conditions == null) return GraphCentricQuery.emptyQuery(resultType);
            } else {
                conditions = new And<>();
                for (List<List<PredicateCondition<String, JanusGraphElement>>> globalConstraint : this.globalConstraints) {
                    Or<JanusGraphElement> or = constructOrCondition(globalConstraint);
                    if (or == null) return GraphCentricQuery.emptyQuery(resultType);
                    conditions.add(or);
                }
            }
        }

        //Prepare orders
        orders.makeImmutable();
        if (orders.isEmpty()) orders = OrderList.NO_ORDER;

        final Set<Condition> coveredClauses = new HashSet<>();
        final IndexSelectionStrategy.SelectedIndexQuery selectedIndex = indexSelector.selectIndices(
            resultType, conditions, coveredClauses, orders, serializer);

        JointIndexQuery indexQuery = selectedIndex.getQuery();
        MultiCondition<JanusGraphElement> queryConditions = conditions;
        if (requiresOrderValues && !orders.isEmpty()
                && (resultType != ElementCategory.VERTEX || hasOrderKeysOfOneValue(orders))) {
            //A value of each order key is a condition of the query, which the indexes chosen for the others take where
            //they can
            final And<JanusGraphElement> withValues = new And<>(conditions.numChildren() + orders.size());
            if (conditions instanceof And) {
                withValues.addAll(conditions);
            } else {
                withValues.add(conditions);
            }
            for (int i = 0; i < orders.size(); i++) {
                final PredicateCondition<PropertyKey, JanusGraphElement> hasValue =
                    new PredicateCondition<>(orders.getKey(i), Cmp.NOT_EQUAL, null);
                if (!withValues.contains(hasValue)) {
                    withValues.add(hasValue);
                    final JointIndexQuery narrowed = requireValues(indexQuery, orders.getKey(i));
                    if (narrowed != indexQuery) {
                        indexQuery = narrowed;
                        coveredClauses.add(hasValue);
                    }
                }
            }
            queryConditions = withValues;
        }

        BackendQueryHolder<JointIndexQuery> query;
        if (!coveredClauses.isEmpty()) {
            int indexLimit;
            if (useSmartLimit) {
                indexLimit = limit == Query.NO_LIMIT ? DEFAULT_NO_LIMIT : Math.min(MAX_BASE_LIMIT, limit);
            } else {
                indexLimit = limit == Query.NO_LIMIT ? hardMaxLimit : limit;
            }
            indexLimit = Math.min(hardMaxLimit, QueryUtil.adjustLimitForTxModifications(tx,
                queryConditions.numChildren() - coveredClauses.size(), indexLimit));
            query = new BackendQueryHolder<>(indexQuery.updateLimit(indexLimit),
                    coveredClauses.size() == queryConditions.numChildren() || coveredClauses.contains(queryConditions),
                    selectedIndex.isSorted());
        } else {
            query = new BackendQueryHolder<>(new JointIndexQuery(), false, selectedIndex.isSorted());
        }
        return new GraphCentricQuery(resultType, queryConditions, orders, query, limit);
    }

    /*
     * Whether every order key has one value at most. Where a key of several values orders the vertices of the query,
     * which only an index can sort them by, a condition on any of its order keys would make the query take the
     * vertices whose value of the key the transaction changed among its new ones, and compare them with the others by
     * each order key in memory, which it can't do by a key of several values.
     */
    private static boolean hasOrderKeysOfOneValue(OrderList orders) {
        for (int i = 0; i < orders.size(); i++) {
            if (orders.getKey(i).cardinality() != Cardinality.SINGLE) {
                return false;
            }
        }
        return true;
    }

    /*
     * The index query with each subquery that reads a mixed index which holds the key, enabled and able to tell which
     * elements have a value of it, leaving out the elements which have none; the given query where no subquery can
     */
    private JointIndexQuery requireValues(JointIndexQuery indexQuery, PropertyKey key) {
        final JointIndexQuery narrowed = new JointIndexQuery();
        boolean anyNarrowed = false;
        for (int i = 0; i < indexQuery.size(); i++) {
            final JointIndexQuery.Subquery subquery = indexQuery.getQuery(i);
            if (subquery.getIndex().isCompositeIndex()) {
                narrowed.add((CompositeIndexType) subquery.getIndex(), subquery.getCompositeQuery());
                continue;
            }
            final MixedIndexType index = (MixedIndexType) subquery.getIndex();
            final IndexQuery mixed = subquery.getMixedQuery();
            final ParameterIndexField field = index.getField(key);
            if (field == null || field.getStatus() != SchemaStatus.ENABLED
                    || !serializer.supportsExistsQuery(index, field)) {
                narrowed.add(index, mixed);
                continue;
            }
            final And<JanusGraphElement> withValue = JointIndexQuery.conjunction(mixed.getCondition(),
                new PredicateCondition<>(IndexRecordUtil.key2Field(field), Cmp.NOT_EQUAL, null));
            narrowed.add(index, new IndexQuery(mixed.getStore(), withValue, ImmutableList.copyOf(mixed.getOrder()),
                mixed.getLimit()));
            anyNarrowed = true;
        }
        return anyNarrowed ? narrowed : indexQuery;
    }

    private Or<JanusGraphElement> constructOrCondition(List<List<PredicateCondition<String, JanusGraphElement>>> globalConstraint) {
        Or<JanusGraphElement> or = new Or<>();
        for (final List<PredicateCondition<String, JanusGraphElement>> child : globalConstraint){
            final And<JanusGraphElement> localconditions = QueryUtil.constraints2QNF(tx, child);
            if (localconditions == null) return null;
            or.add(localconditions);
        }
        return or;
    }
}
