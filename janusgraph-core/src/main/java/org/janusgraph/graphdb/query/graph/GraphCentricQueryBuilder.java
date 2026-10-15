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
//import org.janusgraph.core.Cardinality;
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
     * The sub-queries of each or() call, as they were when it was made. Each or() call holds along with the query's own
     * constraints. None by default.
     */
    private final List<List<SubQuery>> globalConstraints = new ArrayList<>();
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
     * the key, and the query checks it in memory otherwise.
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
        final List<SubQuery> branches = new ArrayList<>(subQueries.size());
        subQueries.forEach(subQuery -> branches.add(new SubQuery(subQuery)));
        this.globalConstraints.add(branches);
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

    /*
     * The indexes which the strategy selected, sorted only where the first of them takes the query's order, each of its
     * keys in its direction: a strategy may hold an index to sort the results to which IndexSerializer gave no order,
     * as it doesn't give an index whose backend can't sort by a key of several values an order by such a key, and a
     * custom strategy which decides by the deprecated IndexSelectionUtil.indexCoversOrder(MixedIndexType, OrderList)
     * would, as would one which gives the index only part of the order. JanusGraph sorts such results.
     */
    private IndexSelectionStrategy.SelectedIndexQuery sortedOnlyWhereTheIndexSorts(
            final IndexSelectionStrategy.SelectedIndexQuery selected) {
        final JointIndexQuery query = selected.getQuery();
        if (!selected.isSorted() || orders.isEmpty() || takesTheOrder(query)) {
            return selected;
        }
        return new IndexSelectionStrategy.SelectedIndexQuery(query, false);
    }

    //Whether the first index of the query is a mixed index which sorts the results by the query's order: by each of its
    //keys in its direction, and by no other key
    private boolean takesTheOrder(final JointIndexQuery query) {
        if (query.isEmpty() || !query.getQuery(0).getIndex().isMixedIndex()) {
            return false;
        }
        final MixedIndexType index = (MixedIndexType) query.getQuery(0).getIndex();
        final List<IndexQuery.OrderEntry> order = query.getQuery(0).getMixedQuery().getOrder();
        if (order.size() != orders.size()) {
            return false;
        }
        for (int i = 0; i < orders.size(); i++) {
            final PropertyKey key = orders.getKey(i);
            if (!index.indexesKey(key) || order.get(i).getOrder() != orders.getOrder(i)
                    || !IndexRecordUtil.key2Field(index, key).equals(order.get(i).getKey())) {
                return false;
            }
        }
        return true;
    }

    public GraphCentricQuery constructQueryWithoutProfile(final ElementCategory resultType) {
        Preconditions.checkNotNull(resultType);
        if (limit == 0) return GraphCentricQuery.emptyQuery(resultType);

        //Prepare constraints, whose conditions compare strings by code point where JanusGraph checks them itself, as
        //the mixed indexes which answer them and the query's order do (see constraints2QNF). The query's own
        //conditions, and those of each or() call with a single sub-query which an element can hold, are one
        //conjunction, along with which each or() of several such sub-queries holds
        final List<Or<JanusGraphElement>> disjunctions = new ArrayList<>(globalConstraints.size());
        final And<JanusGraphElement> conjunction = conjunction(constraints, globalConstraints, disjunctions);
        if (conjunction == null) {
            return GraphCentricQuery.emptyQuery(resultType);
        }
        final MultiCondition<JanusGraphElement> conditions;
        //An or() alone is its disjunction, as before. Next to other conditions, each or() is written flat where its
        //sub-queries have a single condition each, so that the condition stays in the normal form which a composite
        //index, and a mixed index which takes no other form, need to answer any of it
        if (conjunction.isEmpty() && disjunctions.size() == 1) {
            conditions = disjunctions.get(0);
        } else {
            conditions = conjunction;
            for (final Or<JanusGraphElement> or : disjunctions) {
                conditions.add(flattened(or));
            }
        }

        //Prepare orders
        orders.makeImmutable();
        if (orders.isEmpty()) orders = OrderList.NO_ORDER;

        final Set<Condition> coveredClauses = new HashSet<>();
        final IndexSelectionStrategy.SelectedIndexQuery selectedIndex = sortedOnlyWhereTheIndexSorts(
            indexSelector.selectIndices(resultType, conditions, coveredClauses, orders, serializer));

        JointIndexQuery indexQuery = selectedIndex.getQuery();
        MultiCondition<JanusGraphElement> queryConditions = conditions;
        if (requiresOrderValues && !orders.isEmpty()) {
            //A value of each order key is a condition of the query, which the indexes chosen for the others take where
            //they can, unless the query has it already. Another condition on the key doesn't take its place, as an
            //index may find that one in documents without the key: Solr drops the clause of a condition whose value
            //the key's analyzer turns into no terms
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

    /*
     * The conjunction of the conditions of a query, or of a sub-query of an or() call, and of those of each of its or()
     * calls with a single sub-query which an element can hold, without repeating an atom, as constraints2QNF doesn't;
     * each or() call of several such sub-queries is added to the given disjunctions instead. Null where no element can
     * hold the conditions.
     */
    private And<JanusGraphElement> conjunction(final List<PredicateCondition<String, JanusGraphElement>> own,
            final List<List<SubQuery>> ors, final List<Or<JanusGraphElement>> disjunctions) {
        final And<JanusGraphElement> conjunction = QueryUtil.constraints2QNF(tx, own);
        if (conjunction == null) {
            return null;
        }
        final Set<Condition<JanusGraphElement>> atoms = atomsOf(conjunction);
        for (final List<SubQuery> subQueries : ors) {
            final Or<JanusGraphElement> or = disjunction(subQueries);
            if (or == null) {
                return null;
            }
            if (or.size() == 1) {
                for (final Condition<JanusGraphElement> condition : or.get(0).getChildren()) {
                    addOnce(conjunction, atoms, condition);
                }
            } else {
                disjunctions.add(or);
            }
        }
        return conjunction;
    }

    /*
     * The disjunction of the sub-queries of an or() call which an element can hold, each the conjunction of its
     * conditions and of its own or() calls; null where it can hold none, as in an or() of no sub-queries: a sub-query
     * which no element can hold, such as one on a key which doesn't exist, adds nothing to the others. A sub-query
     * which every element holds, as one of no conditions, is the or()'s only one, as the or() holds for every element.
     */
    private Or<JanusGraphElement> disjunction(final List<SubQuery> subQueries) {
        final Or<JanusGraphElement> or = new Or<>(subQueries.size());
        for (final SubQuery subQuery : subQueries) {
            final List<Or<JanusGraphElement>> nested = new ArrayList<>(subQuery.ors.size());
            final And<JanusGraphElement> conditions = conjunction(subQuery.constraints, subQuery.ors, nested);
            if (conditions == null) {
                continue;
            }
            for (final Or<JanusGraphElement> inner : nested) {
                conditions.add(flattened(inner));
            }
            if (conditions.isEmpty()) {
                final Or<JanusGraphElement> always = new Or<>(1);
                always.add(conditions);
                return always;
            }
            or.add(conditions);
        }
        return or.isEmpty() ? null : or;
    }

    /*
     * The disjunction with each branch of a single condition written as that condition, and each disjunction which is
     * one, as that of a within(), written as its own branches, without repeating an atom: next to other conditions, a
     * disjunction of single atoms is then in normal form, as is one of a within(), though not one of a P.or() with an
     * and() in it. A branch of several conditions stays a conjunction: JanusGraph checks it in memory, unless a mixed
     * index which takes conditions that aren't in normal form covers the whole condition, as Elasticsearch and Lucene
     * take them.
     */
    private static Or<JanusGraphElement> flattened(final Or<JanusGraphElement> or) {
        final Or<JanusGraphElement> flat = new Or<>(or.size());
        final Set<Condition<JanusGraphElement>> atoms = new HashSet<>();
        for (final Condition<JanusGraphElement> branch : or.getChildren()) {
            final Condition<JanusGraphElement> single = branch instanceof And && branch.numChildren() == 1
                ? branch.getChildren().iterator().next() : branch;
            for (final Condition<JanusGraphElement> condition
                    : single instanceof Or ? single.getChildren() : Collections.singletonList(single)) {
                addOnce(flat, atoms, condition);
            }
        }
        return flat;
    }

    /*
     * The atoms among the conditions. A set of them tells which conditions an atom repeats, as in
     * QueryUtil.addConstraints: looking each one up in the list would make a long without() quadratic in its values.
     * Only atoms are compared: MultiCondition.equals() takes a conjunction for another one of as many conditions which
     * holds each of its own, so that one of X and X would equal one of X and Y.
     */
    private static Set<Condition<JanusGraphElement>> atomsOf(final MultiCondition<JanusGraphElement> conditions) {
        final Set<Condition<JanusGraphElement>> atoms = new HashSet<>();
        for (final Condition<JanusGraphElement> condition : conditions) {
            if (condition instanceof PredicateCondition) {
                atoms.add(condition);
            }
        }
        return atoms;
    }

    //Adds the condition unless it is an atom which the conditions hold already, as the given atoms tell
    private static void addOnce(final MultiCondition<JanusGraphElement> conditions,
            final Set<Condition<JanusGraphElement>> atoms, final Condition<JanusGraphElement> condition) {
        if (!(condition instanceof PredicateCondition) || atoms.add(condition)) {
            conditions.add(condition);
        }
    }

    /*
     * The conditions of a sub-query of an or() call when the call was made: its own constraints and the sub-queries of
     * its own or() calls, which are kept in the same way, so that a later change of the sub-query changes neither
     */
    private static final class SubQuery {
        private final List<PredicateCondition<String, JanusGraphElement>> constraints;
        private final List<List<SubQuery>> ors;

        private SubQuery(final GraphCentricQueryBuilder query) {
            constraints = new ArrayList<>(query.constraints);
            ors = new ArrayList<>(query.globalConstraints);
        }
    }
}
