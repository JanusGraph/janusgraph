package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.*;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.relations.StandardProperty;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.SchemaStatus;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.thinkaurelius.titan.util.datastructures.ProperInterval;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Builds a {@link BaseVertexQuery}, optimizes the query and compiles the result into a {@link com.thinkaurelius.titan.graphdb.query.vertex.BaseVertexCentricQuery} which
 * is then executed by one of the extending classes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractVertexCentricQueryBuilder<Q extends BaseVertexQuery<Q>> implements BaseVertexQuery<Q> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AbstractVertexCentricQueryBuilder.class);

    private static final String[] NO_TYPES = new String[0];
    private static final List<PredicateCondition<String, TitanRelation>> NO_CONSTRAINTS = ImmutableList.of();

    /**
     * Transaction in which this query is executed
     */
    protected final StandardTitanTx tx;

    /**
     * The direction of this query. BOTH by default
     */
    private Direction dir = Direction.BOTH;
    /**
     * The relation types (labels or keys) to query for. None by default which means query for any relation type.
     */
    private String[] types = NO_TYPES;
    /**
     * The constraints added to this query. None by default.
     */
    private List<PredicateCondition<String, TitanRelation>> constraints = NO_CONSTRAINTS;
    /**
     * The vertex to be used for the adjacent vertex constraint. If null, that means no such constraint. Null by default.
     */
    private TitanVertex adjacentVertex = null;
    /**
     * The order in which the relations should be returned. None by default.
     */
    private OrderList orders = new OrderList();
    /**
     * The limit of this query. No limit by default.
     */
    private int limit = Query.NO_LIMIT;


    public AbstractVertexCentricQueryBuilder(final StandardTitanTx tx) {
        Preconditions.checkArgument(tx!=null);
        this.tx = tx;
    }

    protected abstract Q getThis();

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    @Override
    public Q adjacent(TitanVertex vertex) {
        Preconditions.checkNotNull(vertex);
        this.adjacentVertex = vertex;
        return getThis();
    }

    private Q addConstraint(String type, TitanPredicate rel, Object value) {
        Preconditions.checkArgument(type!=null && StringUtils.isNotBlank(type) && rel!=null);
        if (constraints==NO_CONSTRAINTS) constraints = new ArrayList<PredicateCondition<String, TitanRelation>>(5);
        constraints.add(new PredicateCondition<String, TitanRelation>(type, rel, value));
        return getThis();
    }

    @Override
    public Q has(TitanKey key, Object value) {
        return has(key.getName(), value);
    }

    @Override
    public Q has(TitanLabel label, TitanVertex vertex) {
        return has(label.getName(), vertex);
    }

    @Override
    public Q has(String type, Object value) {
        return addConstraint(type, Cmp.EQUAL, value);
    }

    @Override
    public Q hasNot(String key, Object value) {
        return has(key, Cmp.NOT_EQUAL, value);
    }

    @Override
    public Q has(String key, Predicate predicate, Object value) {
        return addConstraint(key, TitanPredicate.Converter.convert(predicate), value);
    }

    @Override
    public Q has(TitanKey key, Predicate predicate, Object value) {
        return has(key.getName(), predicate, value);
    }

    @Override
    public Q has(String key) {
        return has(key, Cmp.NOT_EQUAL, (Object) null);
    }

    @Override
    public Q hasNot(String key) {
        return has(key, Cmp.EQUAL, (Object) null);
    }

    @Override
    public <T extends Comparable<?>> Q interval(TitanKey key, T start, T end) {
        return interval(key.getName(), start, end);
    }

    @Override
    public <T extends Comparable<?>> Q interval(String key, T start, T end) {
        addConstraint(key, Cmp.GREATER_THAN_EQUAL, start);
        return addConstraint(key, Cmp.LESS_THAN, end);
    }

    @Deprecated
    public <T extends Comparable<T>> Q has(String key, T value, com.tinkerpop.blueprints.Query.Compare compare) {
        return addConstraint(key, TitanPredicate.Converter.convert(compare), value);
    }

    @Override
    public Q types(TitanType... types) {
        String[] ts = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            ts[i]=types[i].getName();
        }
        return types(ts);
    }

    @Override
    public Q labels(String... labels) {
        return types(labels);
    }

    @Override
    public Q keys(String... keys) {
        return types(keys);
    }

    public Q type(TitanType type) {
        return types(type.getName());
    }

    private Q types(String... types) {
        Preconditions.checkArgument(types!=null);
        for (String type : types) Preconditions.checkArgument(StringUtils.isNotBlank(type),"Invalid type: %s",type);
        this.types=types;
        return getThis();
    }

    @Override
    public Q direction(Direction d) {
        Preconditions.checkNotNull(d);
        dir = d;
        return getThis();
    }

    @Override
    public Q limit(int limit) {
        Preconditions.checkArgument(limit >= 0);
        this.limit = limit;
        return getThis();
    }

    @Override
    public Q orderBy(String key, Order order) {
        return orderBy(tx.getPropertyKey(key), order);
    }

    @Override
    public Q orderBy(TitanKey key, Order order) {
        Preconditions.checkArgument(key!=null,"Cannot order on undefined key");
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.getDataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.getName(), key.getDataType());
        Preconditions.checkArgument(key.getCardinality()==Cardinality.SINGLE, "Ordering is undefined on multi-valued key [%s]", key.getName());
        Preconditions.checkArgument(!(key instanceof SystemType),"Cannot use system types in ordering: %s",key);
        Preconditions.checkArgument(!orders.containsKey(key.getName()));
        Preconditions.checkArgument(orders.isEmpty(),"Only a single sort order is supported on vertex queries");
        orders.add(key, order);
        return getThis();
    }


    /* ---------------------------------------------------------------
     * Utility Methods
	 * ---------------------------------------------------------------
	 */

    protected Direction getDirection() {
        return dir;
    }

    protected TitanVertex getAdjacentVertex() {
        return adjacentVertex;
    }

    protected final boolean hasTypes() {
        return types.length>0;
    }

    /**
     * Whether this query is asking for the value of an {@link ImplicitKey}.
     * </p>
     * Handling of implicit keys is completely distinct from "normal" query execution and handled extra
     * for completeness reasons.
     *
     * @param returnType
     * @return
     */
    protected final boolean isImplicitKeyQuery(RelationCategory returnType) {
        if (returnType==RelationCategory.EDGE || types.length!=1 || !constraints.isEmpty()) return false;
        return tx.getType(types[0]) instanceof ImplicitKey;
    }

    /**
     * If {@link #isImplicitKeyQuery(com.thinkaurelius.titan.graphdb.internal.RelationCategory)} is true,
     * this method provides the result set for the query based on the evaluation of the {@link ImplicitKey}.
     * </p>
     * Handling of implicit keys is completely distinct from "normal" query execution and handled extra
     * for completeness reasons.
     *
     * @param v
     * @return
     */
    protected Iterable<TitanRelation> executeImplicitKeyQuery(InternalVertex v) {
        assert isImplicitKeyQuery(RelationCategory.PROPERTY);
        if (dir==Direction.IN || limit<1) return ImmutableList.of();
        ImplicitKey key = (ImplicitKey)tx.getType(types[0]);
        return ImmutableList.of((TitanRelation)new StandardProperty(0,key,v,key.computeProperty(v), v.isNew()?ElementLifeCycle.New:ElementLifeCycle.Loaded));
    }

    protected static Iterable<TitanVertex> edges2Vertices(final Iterable<TitanEdge> edges, final TitanVertex other) {
        return Iterables.transform(edges, new Function<TitanEdge, TitanVertex>() {
            @Nullable
            @Override
            public TitanVertex apply(@Nullable TitanEdge titanEdge) {
                return titanEdge.getOtherVertex(other);
            }
        });
    }

    protected VertexList edges2VertexIds(final Iterable<TitanEdge> edges, final TitanVertex other) {
        VertexArrayList vertices = new VertexArrayList();
        for (TitanEdge edge : edges) vertices.add(edge.getOtherVertex(other));
        return vertices;
    }

    /* ---------------------------------------------------------------
     * Query Optimization
	 * ---------------------------------------------------------------
	 */

    private static final int HARD_MAX_LIMIT   = 300000;

    protected BaseVertexCentricQuery constructQuery(RelationCategory returnType) {
        assert returnType != null;
        Preconditions.checkArgument(adjacentVertex==null || returnType == RelationCategory.EDGE,"Vertex constraints only apply to edges");
        if (limit <= 0)
            return BaseVertexCentricQuery.emptyQuery();

        //Prepare direction
        if (returnType == RelationCategory.PROPERTY) {
            if (dir == Direction.IN)
                return BaseVertexCentricQuery.emptyQuery();
            dir = Direction.OUT;
        }
        //Prepare order
        orders.makeImmutable();
        assert orders.hasCommonOrder();

        //Prepare constraints
        And<TitanRelation> conditions = QueryUtil.constraints2QNF(tx, constraints);
        if (conditions == null)
            return BaseVertexCentricQuery.emptyQuery();

        //Don't be smart with query limit adjustments - it just messes up the caching layer and penalizes when appropriate limits are set by the user!
        int sliceLimit = limit;

        //Construct (optimal) SliceQueries
        EdgeSerializer serializer = tx.getEdgeSerializer();
        List<BackendQueryHolder<SliceQuery>> queries;
        if (!hasTypes()) {
            BackendQueryHolder<SliceQuery> query = new BackendQueryHolder<SliceQuery>(serializer.getQuery(returnType),
                    ((dir == Direction.BOTH || (returnType == RelationCategory.PROPERTY && dir == Direction.OUT))
                            && !conditions.hasChildren()), orders.isEmpty(), null);
            if (sliceLimit!=Query.NO_LIMIT && sliceLimit<Integer.MAX_VALUE/3) {
                //If only one direction is queried, ask for twice the limit from backend since approximately half will be filtered
                if (dir != Direction.BOTH && (returnType == RelationCategory.EDGE || returnType == RelationCategory.RELATION))
                    sliceLimit *= 2;
            }
            query.getBackendQuery().setLimit(computeLimit(conditions.size(),sliceLimit));
            queries = ImmutableList.of(query);
            conditions.add(returnType);
            conditions.add(new HiddenFilterCondition<TitanRelation>()); //Need this to filter out newly created hidden relations in the transaction
        } else {
            Set<TitanType> ts = new HashSet<TitanType>(types.length);
            queries = new ArrayList<BackendQueryHolder<SliceQuery>>(types.length + 2);
            Map<TitanType,ProperInterval> intervalConstraints = new HashMap<TitanType, ProperInterval>(conditions.size());
            final boolean isIntervalFittedConditions = compileConstraints(conditions,intervalConstraints);
            for (ProperInterval pint : intervalConstraints.values()) { //Check if one of the constraints leads to an empty result set
                if (pint.isEmpty()) return BaseVertexCentricQuery.emptyQuery();
            }

            for (String typeName : types) {
                InternalType type = QueryUtil.getType(tx, typeName);
                if (type==null) continue;
                if (type instanceof ImplicitKey) throw new UnsupportedOperationException("Implicit types are not supported in complex queries: "+type);
                ts.add(type);

                if (type.isPropertyKey()) {
                    if (returnType == RelationCategory.EDGE)
                        throw new IllegalArgumentException("Querying for edges but including a property key: " + type.getName());
                    returnType = RelationCategory.PROPERTY;
                }
                if (type.isEdgeLabel()) {
                    if (returnType == RelationCategory.PROPERTY)
                        throw new IllegalArgumentException("Querying for properties but including an edge label: " + type.getName());
                    returnType = RelationCategory.EDGE;
                }


                if (type.isEdgeLabel() && dir==Direction.BOTH && intervalConstraints.isEmpty() && orders.isEmpty()) {
                    //TODO: This if-condition is a little too restrictive - we also want to include those cases where there
                    // ARE intervalConstraints or orders but those cannot be covered by any sort-keys
                    SliceQuery q = serializer.getQuery(type, dir, null);
                    q.setLimit(sliceLimit);
                    queries.add(new BackendQueryHolder<SliceQuery>(q, isIntervalFittedConditions, true, null));
                } else {
                    //Optimize for each direction independently
                    Direction[] dirs = {dir};
                    if (dir == Direction.BOTH) {
                        if (type.isEdgeLabel())
                            dirs = new Direction[]{Direction.OUT, Direction.IN};
                        else
                            dirs = new Direction[]{Direction.OUT}; //property key
                    }

                    for (Direction direction : dirs) {
                        /*
                        Find best scoring relation type to answer this query with. We score each candidate by the number
                        of conditions that each sort-keys satisfy. Equality conditions score higher than interval conditions
                        since they are more restrictive. We assign additional points if the sort key satisfies the order
                        of this query.
                        */
                        InternalType bestCandidate = null;
                        int bestScore = Integer.MIN_VALUE;
                        boolean bestCandidateSupportsOrder = false;
                        for (InternalType candidate : type.getRelationIndexes()) {
                            //Filter out those that don't apply
                            if (!candidate.isUnidirected(Direction.BOTH) && !candidate.isUnidirected(direction)) continue;
                            if (candidate.getStatus()!= SchemaStatus.ENABLED) continue;

                            boolean supportsOrder = orders.isEmpty()?true:orders.getCommonOrder()==candidate.getSortOrder();
                            int currentOrder = 0;

                            int score = 0;
                            TitanType[] extendedSortKey = getExtendedSortKey(candidate,direction,tx);

                            for (int i=0;i<extendedSortKey.length;i++) {
                                TitanType keyType = extendedSortKey[i];
                                if (currentOrder<orders.size() && orders.getKey(currentOrder).equals(keyType)) currentOrder++;

                                ProperInterval interval = intervalConstraints.get(keyType);
                                if (interval==null || !interval.isPoint()) {
                                    if (interval!=null) score+=1;
                                    break;
                                } else {
                                    assert interval.isPoint();
                                    score+=5;
                                }
                            }
                            if (supportsOrder && currentOrder==orders.size()) score+=3;
                            if (score>bestScore) {
                                bestScore=score;
                                bestCandidate=candidate;
                                bestCandidateSupportsOrder=supportsOrder && currentOrder==orders.size();
                            }
                        }
                        Preconditions.checkArgument(bestCandidate!=null,"Current graph schema does not support the specified query constraints for type: %s",type.getName());

                        //Construct sort key constraints for the best candidate and then serialize into a SliceQuery
                        //that is wrapped into a BackendQueryHolder
                        TitanType[] extendedSortKey = getExtendedSortKey(bestCandidate,direction,tx);
                        EdgeSerializer.TypedInterval[] sortKeyConstraints = new EdgeSerializer.TypedInterval[extendedSortKey.length];
                        int coveredTypes = 0;
                        for (int i = 0; i < extendedSortKey.length; i++) {
                            TitanType keyType = extendedSortKey[i];
                            ProperInterval interval = intervalConstraints.get(keyType);
                            if (interval!=null) {
                                sortKeyConstraints[i]=new EdgeSerializer.TypedInterval((InternalType) keyType,interval);
                                coveredTypes++;
                            }
                            if (interval==null || !interval.isPoint()) break;
                        }

                        boolean isFitted = isIntervalFittedConditions && coveredTypes==intervalConstraints.size();
                        SliceQuery q = serializer.getQuery(bestCandidate, direction, sortKeyConstraints);
                        q.setLimit(computeLimit(intervalConstraints.size()-coveredTypes, sliceLimit));
                        queries.add(new BackendQueryHolder<SliceQuery>(q, isFitted, bestCandidateSupportsOrder, null));


                    }
                }
            }
            if (queries.isEmpty())
                return BaseVertexCentricQuery.emptyQuery();

            conditions.add(getTypeCondition(ts));
        }

        return new BaseVertexCentricQuery(QueryUtil.simplifyQNF(conditions), dir, queries, orders, limit);
    }

    /**
     * Returns the extended sort key of the given type. The extended sort key extends the type's primary sort key
     * by ADJACENT_ID and ID depending on the multiplicity of the type in the given direction.
     * It also converts the type ids to actual types.
     *
     * @param type
     * @param dir
     * @param tx
     * @return
     */
    private static TitanType[] getExtendedSortKey(InternalType type, Direction dir, StandardTitanTx tx) {
        int additional = 0;
        if (!type.getMultiplicity().isUnique(dir)) {
            if (!type.getMultiplicity().isConstrained()) additional++;
            if (type.isEdgeLabel()) additional++;
        }
        TitanType[] entireKey = new TitanType[type.getSortKey().length+additional];
        int i;
        for (i=0;i<type.getSortKey().length;i++) {
            entireKey[i]=tx.getExistingType(type.getSortKey()[i]);
        }
        if (type.isEdgeLabel() && !type.getMultiplicity().isUnique(dir)) entireKey[i++]=ImplicitKey.ADJACENT_ID;
        if (!type.getMultiplicity().isConstrained()) entireKey[i++]=ImplicitKey.ID;
        return entireKey;
    }

    /**
     * Converts the constraint conditions of this query into a constraintMap which is passed as an argument.
     * If all the constraint conditions could be accounted for in the constraintMap, this method returns true, else -
     * if some constraints cannot be captured in an interval - it returns false to indicate that further in-memory filtering
     * will be necessary.
     * </p>
     * This constraint map is used in constructing the SliceQueries and query optimization since this representation
     * is easier to handle.
     *
     * @param conditions
     * @param constraintMap
     * @return
     */
    private boolean compileConstraints(And<TitanRelation> conditions, Map<TitanType,ProperInterval> constraintMap) {
        boolean isFitted = true;
        for (Condition<TitanRelation> condition : conditions.getChildren()) {
            if (!(condition instanceof PredicateCondition)) continue; //TODO: Should we optimize OR clauses?
            PredicateCondition<TitanType, TitanRelation> atom = (PredicateCondition)condition;
            TitanType type = atom.getKey();
            assert type!=null;
            ProperInterval pi = constraintMap.get(type);
            if (pi==null) {
                pi = new ProperInterval();
                constraintMap.put(type,pi);
            }
            boolean fittedSub = compileConstraint(pi,type,atom.getPredicate(),atom.getValue());
            isFitted = isFitted && fittedSub;
        }
        if (adjacentVertex!=null) {
            if (adjacentVertex.hasId()) constraintMap.put(ImplicitKey.ADJACENT_ID,new ProperInterval(adjacentVertex.getID()));
            else isFitted=false;
        }
        return isFitted;
    }

    private static boolean compileConstraint(ProperInterval pint, TitanType type, TitanPredicate predicate, Object value) {
        if (predicate instanceof Cmp) {
            Cmp cmp = (Cmp)predicate;
            if (cmp==Cmp.EQUAL) {
                if (value==null) return false;
                boolean fitted=pint.contains(value);
                pint.setPoint(value);
                return fitted;
            }
            if (cmp==Cmp.NOT_EQUAL) {
                return false;
            }
            assert value!=null && value instanceof Comparable;
            Comparable v = (Comparable)value;
            switch ((Cmp) predicate) {
                case LESS_THAN:
                    if (pint.getEnd() == null || v.compareTo(pint.getEnd()) <= 0) {
                        pint.setEnd(v);
                        pint.setEndInclusive(false);
                    }
                    return true;
                case LESS_THAN_EQUAL:
                    if (pint.getEnd() == null || v.compareTo(pint.getEnd()) < 0) {
                        pint.setEnd(v);
                        pint.setEndInclusive(true);
                    }
                    return true;
                case GREATER_THAN:
                    if (pint.getStart() == null || v.compareTo(pint.getStart()) >= 0) {
                        pint.setStart(v);
                        pint.setStartInclusive(false);
                    }
                    return true;
                case GREATER_THAN_EQUAL:
                    if (pint.getStart() == null || v.compareTo(pint.getStart()) > 0) {
                        pint.setStart(v);
                        pint.setStartInclusive(true);
                    }
                    return true;
                default: throw new AssertionError();
            }
        } else return false;
    }

    /**
     * Constructs a condition that is equivalent to the type constraints of this query if there are any.
     *
     * @param types
     * @return
     */
    private static Condition<TitanRelation> getTypeCondition(Set<TitanType> types) {
        assert !types.isEmpty();
        if (types.size() == 1)
            return new RelationTypeCondition<TitanRelation>(types.iterator().next());

        Or<TitanRelation> typeCond = new Or<TitanRelation>(types.size());
        for (TitanType type : types)
            typeCond.add(new RelationTypeCondition<TitanRelation>(type));

        return typeCond;
    }

    /**
     * Updates a given user limit based on the number of conditions that can not be fulfilled by the backend query, i.e. the query
     * is not fitted and these remaining conditions must be enforced by filtering in-memory. By filtering in memory, we will discard
     * results returned from the backend and hence we should increase the limit to account for this "waste" in order to not have
     * to adjust the limit too often in {@link com.thinkaurelius.titan.graphdb.query.LimitAdjustingIterator}.
     *
     * @param remainingConditions
     * @param baseLimit
     * @return
     */
    private int computeLimit(int remainingConditions, int baseLimit) {
        if (baseLimit==Query.NO_LIMIT) return baseLimit;
        assert baseLimit>0;
        baseLimit = Math.max(baseLimit,Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, remainingConditions, baseLimit)));
        assert baseLimit>0;
        return baseLimit;
    }

    protected static<E extends TitanElement> Condition<E> addAndCondition(Condition<E> base, Condition<E> add) {
        And<E> newcond = (base instanceof And) ? (And) base : new And<E>(base);
        newcond.add(add);
        return newcond;
    }

}
