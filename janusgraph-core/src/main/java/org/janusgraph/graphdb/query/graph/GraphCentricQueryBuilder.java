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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.*;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.*;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.StreamSupport;

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
    private final List<PredicateCondition<String, JanusGraphElement>> constraints;
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

    public GraphCentricQueryBuilder(StandardJanusGraphTx tx, IndexSerializer serializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx = tx;
        this.serializer = serializer;
        this.constraints = new ArrayList<>(5);
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    public GraphCentricQueryBuilder profiler(QueryProfiler profiler) {
        Preconditions.checkNotNull(profiler);
        this.profiler=profiler;
        return this;
    }

    @Override
    public GraphCentricQueryBuilder has(String key, JanusGraphPredicate predicate, Object condition) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(predicate);
        Preconditions.checkArgument(predicate.isValidCondition(condition), "Invalid condition: %s", condition);
        if (predicate.equals(Contain.NOT_IN)) {
            // when querying `has(key, without(value))`, the query must also satisfy `has(key)`
            has(key);
        }
        constraints.add(new PredicateCondition<String, JanusGraphElement>(key, predicate, condition));
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
        PropertyKey key = tx.getPropertyKey(keyName);
        Preconditions.checkArgument(key!=null && order!=null,"Need to specify and key and an order");
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.dataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.name(), key.dataType());
        Preconditions.checkArgument(key.cardinality()== Cardinality.SINGLE,
                "Ordering is undefined on multi-valued key [%s]", key.name());
        Preconditions.checkArgument(!orders.containsKey(key));
        orders.add(key, Order.convert(order));
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    @Override
    public Iterable<JanusGraphVertex> vertices() {
        GraphCentricQuery query = constructQuery(ElementCategory.VERTEX);
        return Iterables.filter(new QueryProcessor<>(query, tx.elementProcessor), JanusGraphVertex.class);
    }

    @Override
    public Iterable<JanusGraphEdge> edges() {
        GraphCentricQuery query = constructQuery(ElementCategory.EDGE);
        return Iterables.filter(new QueryProcessor<>(query, tx.elementProcessor), JanusGraphEdge.class);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> properties() {
        GraphCentricQuery query = constructQuery(ElementCategory.PROPERTY);
        return Iterables.filter(new QueryProcessor<>(query, tx.elementProcessor), JanusGraphVertexProperty.class);
    }


    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    private static final int DEFAULT_NO_LIMIT = 1000;
    private static final int MAX_BASE_LIMIT = 20000;
    private static final int HARD_MAX_LIMIT = 100000;

    private static final double EQUAL_CONDITION_SCORE = 4;
    private static final double OTHER_CONDITION_SCORE = 1;
    private static final double ORDER_MATCH = 2;
    private static final double ALREADY_MATCHED_ADJUSTOR = 0.1;
    private static final double CARDINALITY_SINGE_SCORE = 1000;
    private static final double CARDINALITY_OTHER_SCORE = 1000;


    public GraphCentricQuery constructQuery(final ElementCategory resultType) {
        QueryProfiler optProfiler = profiler.addNested(QueryProfiler.OPTIMIZATION);
        optProfiler.startTimer();
        GraphCentricQuery query = constructQueryWithoutProfile(resultType);
        optProfiler.stopTimer();
        query.observeWith(profiler);
        return query;
    }

    public GraphCentricQuery constructQueryWithoutProfile(final ElementCategory resultType) {
        Preconditions.checkNotNull(resultType);
        if (limit == 0) return GraphCentricQuery.emptyQuery(resultType);

        //Prepare constraints
        And<JanusGraphElement> conditions = QueryUtil.constraints2QNF(tx, constraints);
        if (conditions == null) return GraphCentricQuery.emptyQuery(resultType);

        //Prepare orders
        orders.makeImmutable();
        if (orders.isEmpty()) orders = OrderList.NO_ORDER;

        //Compile all indexes that cover at least one of the query conditions
        final Set<IndexType> indexCandidates = new HashSet<>();
        ConditionUtil.traversal(conditions, condition -> {
            if (condition instanceof PredicateCondition) {
                final RelationType type = ((PredicateCondition<RelationType,JanusGraphElement>) condition).getKey();
                Preconditions.checkArgument(type != null && type.isPropertyKey());
                Iterables.addAll(indexCandidates, Iterables.filter(((InternalRelationType) type).getKeyIndexes(),
                    indexType -> indexType.getElement() == resultType));
            }
            return true;
        });

        /*
        Determine the best join index query to answer this query:
        Iterate over all potential indexes (as compiled above) and compute a score based on how many clauses
        this index covers. The index with the highest score (as long as it covers at least one additional clause)
        is picked and added to the joint query for as long as such exist.
         */
        JointIndexQuery jointQuery = new JointIndexQuery();
        boolean isSorted = orders.isEmpty();
        Set<Condition> coveredClauses = Sets.newHashSet();
        while (true) {
            IndexType bestCandidate = null;
            double candidateScore = 0.0;
            Set<Condition> candidateSubcover = null;
            boolean candidateSupportsSort = false;
            Object candidateSubCondition = null;

            for (IndexType index : indexCandidates) {
                Set<Condition> subcover = Sets.newHashSet();
                Object subCondition;
                boolean supportsSort = orders.isEmpty();
                //Check that this index actually applies in case of a schema constraint
                if (index.hasSchemaTypeConstraint()) {
                    JanusGraphSchemaType type = index.getSchemaTypeConstraint();
                    Map.Entry<Condition,Collection<Object>> equalCon
                            = getEqualityConditionValues(conditions,ImplicitKey.LABEL);
                    if (equalCon==null) continue;
                    Collection<Object> labels = equalCon.getValue();
                    assert labels.size() >= 1;
                    if (labels.size()>1) {
                        log.warn("The query optimizer currently does not support multiple label constraints in query: {}",this);
                        continue;
                    }
                    if (!type.name().equals(Iterables.getOnlyElement(labels))) {
                        continue;
                    }
                    subcover.add(equalCon.getKey());
                }

                if (index.isCompositeIndex()) {
                    subCondition = indexCover((CompositeIndexType) index,conditions,subcover);
                } else {
                    subCondition = indexCover((MixedIndexType) index,conditions,serializer,subcover);
                    if (coveredClauses.isEmpty() && !supportsSort
                            && indexCoversOrder((MixedIndexType)index,orders)) supportsSort=true;
                }
                if (subCondition==null) continue;
                assert !subcover.isEmpty();
                double score = 0.0;
                boolean coversAdditionalClause = false;
                for (Condition c : subcover) {
                    double s = (c instanceof PredicateCondition && ((PredicateCondition)c).getPredicate()==Cmp.EQUAL)?
                            EQUAL_CONDITION_SCORE:OTHER_CONDITION_SCORE;
                    if (coveredClauses.contains(c)) s=s*ALREADY_MATCHED_ADJUSTOR;
                    else coversAdditionalClause = true;
                    score+=s;
                    if (index.isCompositeIndex())
                        score+=((CompositeIndexType)index).getCardinality()==Cardinality.SINGLE?
                                CARDINALITY_SINGE_SCORE:CARDINALITY_OTHER_SCORE;
                }
                if (supportsSort) score+=ORDER_MATCH;
                if (coversAdditionalClause && score>candidateScore) {
                    candidateScore=score;
                    bestCandidate=index;
                    candidateSubcover = subcover;
                    candidateSubCondition = subCondition;
                    candidateSupportsSort = supportsSort;
                }
            }
            if (bestCandidate!=null) {
                if (coveredClauses.isEmpty()) isSorted=candidateSupportsSort;
                coveredClauses.addAll(candidateSubcover);
                if (bestCandidate.isCompositeIndex()) {
                    jointQuery.add((CompositeIndexType)bestCandidate,
                            serializer.getQuery((CompositeIndexType)bestCandidate,(List<Object[]>)candidateSubCondition));
                } else {
                    jointQuery.add((MixedIndexType)bestCandidate,
                            serializer.getQuery((MixedIndexType)bestCandidate,(Condition)candidateSubCondition,orders));
                }
            } else {
                break;
            }
            /* TODO: smarter optimization:
            - use in-memory histograms to estimate selectivity of PredicateConditions and filter out low-selectivity ones
                    if they would result in an individual index call (better to filter afterwards in memory)
            - move OR's up and extend GraphCentricQuery to allow multiple JointIndexQuery for proper or'ing of queries
            */
        }

        BackendQueryHolder<JointIndexQuery> query;
        if (!coveredClauses.isEmpty()) {
            int indexLimit = limit == Query.NO_LIMIT ? HARD_MAX_LIMIT : limit;
            if (tx.getGraph().getConfiguration().adjustQueryLimit()) {
                indexLimit = limit == Query.NO_LIMIT ? DEFAULT_NO_LIMIT : Math.min(MAX_BASE_LIMIT, limit);
            }
            indexLimit = Math.min(HARD_MAX_LIMIT,
                QueryUtil.adjustLimitForTxModifications(tx, coveredClauses.size(), indexLimit));
            jointQuery.setLimit(indexLimit);
            query = new BackendQueryHolder<>(jointQuery,
                    coveredClauses.size() == conditions.numChildren(), isSorted);
        } else {
            query = new BackendQueryHolder<>(new JointIndexQuery(), false, isSorted);
        }
        return new GraphCentricQuery(resultType, conditions, orders, query, limit);
    }

    public static boolean indexCoversOrder(MixedIndexType index, OrderList orders) {
        for (int i = 0; i < orders.size(); i++) {
            if (!index.indexesKey(orders.getKey(i))) return false;
        }
        return true;
    }

    public static List<Object[]> indexCover(final CompositeIndexType index, Condition<JanusGraphElement> condition,
                                            Set<Condition> covered) {
        assert QueryUtil.isQueryNormalForm(condition);
        assert condition instanceof And;
        if (index.getStatus()!= SchemaStatus.ENABLED) return null;
        IndexField[] fields = index.getFieldKeys();
        Object[] indexValues = new Object[fields.length];
        final Set<Condition> coveredClauses = new HashSet<>(fields.length);
        final List<Object[]> indexCovers = new ArrayList<>(4);

        constructIndexCover(indexValues,0,fields,condition,indexCovers,coveredClauses);
        if (!indexCovers.isEmpty()) {
            covered.addAll(coveredClauses);
            return indexCovers;
        } else return null;
    }

    private static void constructIndexCover(Object[] indexValues, int position, IndexField[] fields,
                                            Condition<JanusGraphElement> condition,
                                            List<Object[]> indexCovers, Set<Condition> coveredClauses) {
        if (position>=fields.length) {
            indexCovers.add(indexValues);
        } else {
            IndexField field = fields[position];
            Map.Entry<Condition,Collection<Object>> equalCon = getEqualityConditionValues(condition,field.getFieldKey());
            if (equalCon!=null) {
                coveredClauses.add(equalCon.getKey());
                assert equalCon.getValue().size()>0;
                for (Object value : equalCon.getValue()) {
                    Object[] newValues = Arrays.copyOf(indexValues,fields.length);
                    newValues[position]=value;
                    constructIndexCover(newValues,position+1,fields,condition,indexCovers,coveredClauses);
                }
            }
        }
    }

    private static Map.Entry<Condition,Collection<Object>> getEqualityConditionValues(
            Condition<JanusGraphElement> condition, RelationType type) {
        for (Condition c : condition.getChildren()) {
            if (c instanceof Or) {
                Map.Entry<RelationType,Collection> orEqual = QueryUtil.extractOrCondition((Or)c);
                if (orEqual!=null && orEqual.getKey().equals(type) && !orEqual.getValue().isEmpty()) {
                    return new AbstractMap.SimpleImmutableEntry(c,orEqual.getValue());
                }
            } else if (c instanceof PredicateCondition) {
                PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition)c;
                if (atom.getKey().equals(type) && atom.getPredicate()==Cmp.EQUAL && atom.getValue()!=null) {
                    return new AbstractMap.SimpleImmutableEntry(c,ImmutableList.of(atom.getValue()));
                }
            }

        }
        return null;
    }

    public static Condition<JanusGraphElement> indexCover(final MixedIndexType index,
                                                          Condition<JanusGraphElement> condition,
                                                          final IndexSerializer indexInfo,
                                                          final Set<Condition> covered) {
        assert QueryUtil.isQueryNormalForm(condition);
        assert condition instanceof And;
        final And<JanusGraphElement> subCondition = new And<>(condition.numChildren());
        for (Condition<JanusGraphElement> subClause : condition.getChildren()) {
            if (coversAll(index,subClause,indexInfo)) {
                subCondition.add(subClause);
                covered.add(subClause);
            }
        }
        return subCondition.isEmpty()?null:subCondition;
    }

    private static boolean coversAll(final MixedIndexType index, Condition<JanusGraphElement> condition,
                                     IndexSerializer indexInfo) {
        if (condition.getType()!=Condition.Type.LITERAL) {
            return StreamSupport.stream(condition.getChildren().spliterator(), false)
                .allMatch(child -> coversAll(index, child, indexInfo));
        }
        if (!(condition instanceof PredicateCondition)) {
            return false;
        }
        PredicateCondition<RelationType, JanusGraphElement> atom = (PredicateCondition) condition;
        if (atom.getValue() == null) {
            return false;
        }

        Preconditions.checkArgument(atom.getKey().isPropertyKey());
        final PropertyKey key = (PropertyKey) atom.getKey();
        final ParameterIndexField[] fields = index.getFieldKeys();
        final ParameterIndexField match = Arrays.stream(fields)
            .filter(field -> field.getStatus() == SchemaStatus.ENABLED)
            .filter(field -> field.getFieldKey().equals(key))
            .findAny().orElse(null);
        return match != null && indexInfo.supports(index, match, atom.getPredicate());
    }


}
