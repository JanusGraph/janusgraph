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
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.SchemaStatus;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.util.datastructures.ProperInterval;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

public abstract class AbstractVertexCentricQueryBuilder implements BaseVertexQuery {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AbstractVertexCentricQueryBuilder.class);

    private static final String[] NO_TYPES = new String[0];
    private static final List<PredicateCondition<String, TitanRelation>> NO_CONSTRAINTS = ImmutableList.of();

    protected final StandardTitanTx tx;

    //Initial query configuration
    private Direction dir = Direction.BOTH;
    private String[] types = NO_TYPES;
    private List<PredicateCondition<String, TitanRelation>> constraints = NO_CONSTRAINTS;

    private int limit = Query.NO_LIMIT;


    public AbstractVertexCentricQueryBuilder(final StandardTitanTx tx) {
        Preconditions.checkArgument(tx!=null);
        this.tx = tx;
    }

    Direction getDirection() {
        return dir;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */


    private AbstractVertexCentricQueryBuilder addConstraint(String type, TitanPredicate rel, Object value) {
        Preconditions.checkArgument(type!=null && StringUtils.isNotBlank(type) && rel!=null);
        if (constraints==NO_CONSTRAINTS) constraints = new ArrayList<PredicateCondition<String, TitanRelation>>(5);
        constraints.add(new PredicateCondition<String, TitanRelation>(type, rel, value));
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(TitanKey key, Object value) {
        return has(key.getName(), value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(TitanLabel label, TitanVertex vertex) {
        return has(label.getName(), vertex);
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(String type, Object value) {
        return addConstraint(type, Cmp.EQUAL, value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder hasNot(String key, Object value) {
        return has(key, Cmp.NOT_EQUAL, value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(String key, Predicate predicate, Object value) {
        return addConstraint(key, TitanPredicate.Converter.convert(predicate), value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(TitanKey key, Predicate predicate, Object value) {
        return has(key.getName(), predicate, value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(String key) {
        return has(key, Cmp.NOT_EQUAL, (Object) null);
    }

    @Override
    public AbstractVertexCentricQueryBuilder hasNot(String key) {
        return has(key, Cmp.EQUAL, (Object) null);
    }

    @Override
    public <T extends Comparable<?>> AbstractVertexCentricQueryBuilder interval(TitanKey key, T start, T end) {
        return interval(key.getName(), start, end);
    }

    @Override
    public <T extends Comparable<?>> AbstractVertexCentricQueryBuilder interval(String key, T start, T end) {
        addConstraint(key, Cmp.GREATER_THAN_EQUAL, start);
        return addConstraint(key, Cmp.LESS_THAN, end);
    }

    @Deprecated
    public <T extends Comparable<T>> AbstractVertexCentricQueryBuilder has(String key, T value, com.tinkerpop.blueprints.Query.Compare compare) {
        return addConstraint(key, TitanPredicate.Converter.convert(compare), value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder types(TitanType... types) {
        String[] ts = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            ts[i]=types[i].getName();
        }
        return types(ts);
    }

    @Override
    public AbstractVertexCentricQueryBuilder labels(String... labels) {
        return types(labels);
    }

    @Override
    public AbstractVertexCentricQueryBuilder keys(String... keys) {
        return types(keys);
    }

    public AbstractVertexCentricQueryBuilder type(TitanType type) {
        return types(type.getName());
    }

    private AbstractVertexCentricQueryBuilder types(String... types) {
        Preconditions.checkArgument(types!=null);
        for (String type : types) Preconditions.checkArgument(StringUtils.isNotBlank(type),"Invalid type: %s",type);
        this.types=types;
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder direction(Direction d) {
        Preconditions.checkNotNull(d);
        dir = d;
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder limit(int limit) {
        Preconditions.checkArgument(limit >= 0);
        this.limit = limit;
        return this;
    }

    /* ---------------------------------------------------------------
     * Utility Methods
	 * ---------------------------------------------------------------
	 */

    protected final boolean hasTypes() {
        return types.length>0;
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

    protected EdgeSerializer.VertexConstraint getVertexConstraint() {
        return null;
    }

    private static final int HARD_MAX_LIMIT   = 300000;

    protected BaseVertexCentricQuery constructQuery(RelationCategory returnType) {
        assert returnType != null;
        if (limit == 0)
            return BaseVertexCentricQuery.emptyQuery();

        //Prepare direction
        if (returnType == RelationCategory.PROPERTY) {
            if (dir == Direction.IN)
                return BaseVertexCentricQuery.emptyQuery();

            dir = Direction.OUT;
        }

        Preconditions.checkArgument(getVertexConstraint() == null || returnType == RelationCategory.EDGE,"Vertex constraints only apply to edges");

        //Prepare constraints
        And<TitanRelation> conditions = QueryUtil.constraints2QNF(tx, constraints);
        if (conditions == null)
            return BaseVertexCentricQuery.emptyQuery();

        assert limit > 0;
        //Don't be smart with query limit adjustments - it just messes up the caching layer and penalizes when appropriate limits are set by the user!
        int sliceLimit = limit;

        //Construct (optimal) SliceQueries
        EdgeSerializer serializer = tx.getEdgeSerializer();
        List<BackendQueryHolder<SliceQuery>> queries;
        if (!hasTypes()) {
            BackendQueryHolder<SliceQuery> query = new BackendQueryHolder<SliceQuery>(serializer.getQuery(returnType),
                    ((dir == Direction.BOTH || (returnType == RelationCategory.PROPERTY && dir == Direction.OUT))
                            && !conditions.hasChildren()), true, null);
            if (sliceLimit!=Query.NO_LIMIT && sliceLimit<Integer.MAX_VALUE/3) {
                //If only one direction is queried, ask for twice the limit from backend since approximately half will be filtered
                if (dir != Direction.BOTH && (returnType == RelationCategory.EDGE || returnType == RelationCategory.RELATION))
                    sliceLimit *= 2;
            }
            query.getBackendQuery().setLimit(computeLimit(conditions.size(),sliceLimit));
            queries = ImmutableList.of(query);
            conditions.add(returnType);
            conditions.add(new HiddenFilterCondition<TitanRelation>());
        } else {
            Set<TitanType> ts = new HashSet<TitanType>(types.length);
            queries = new ArrayList<BackendQueryHolder<SliceQuery>>(types.length + 2);
            Map<TitanType,ProperInterval> intervalConstraints = new HashMap<TitanType, ProperInterval>(conditions.size());
            final boolean isIntervalFittedConditions = compileConstraints(conditions,intervalConstraints);
            for (ProperInterval pint : intervalConstraints.values()) { //Check if one of the constraints leads to an empty result set
                if (pint.isEmpty()) return BaseVertexCentricQuery.emptyQuery();
            }
            EdgeSerializer.VertexConstraint vertexConstraint = getVertexConstraint();


            for (String typeName : types) {
                InternalType type = QueryUtil.getType(tx, typeName);
                if (type==null || (type instanceof ImplicitKey)) continue;
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


                //Find best scoring relation type
                InternalType bestCandidate = null;
                int bestScore = Integer.MIN_VALUE;
                for (InternalType candidate : type.getRelationIndexes()) {
                    if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(dir)) continue;
                    if (type.getStatus()!= SchemaStatus.ENABLED) continue;

                    int score = 0;
                    boolean coveredAllSortKeys = true;
                    for (long keyid : candidate.getSortKey()) {
                        TitanType keyType = tx.getExistingType(keyid);
                        ProperInterval interval = intervalConstraints.get(keyType);
                        if (interval==null || !interval.isPoint()) {
                            if (interval!=null) score+=1;
                            coveredAllSortKeys=false;
                            break;
                        } else {
                            assert interval.isPoint();
                            score+=2;
                        }
                    }
                    if (coveredAllSortKeys && vertexConstraint!=null) score+=10;
                    if (score>bestScore) {
                        bestScore=score;
                        bestCandidate=candidate;
                    }
                }
                Preconditions.checkArgument(bestCandidate!=null,"Current graph schema does not support the specified query constraints for type: %s",type.getName());

                //Construct sort key constraints
                long[] sortKey = bestCandidate.getSortKey();
                EdgeSerializer.TypedInterval[] sortKeyConstraints = new EdgeSerializer.TypedInterval[sortKey.length];
                int coveredTypes = 0;
                for (int i = 0; i < sortKeyConstraints.length; i++) {
                    TitanType keyType = tx.getExistingType(sortKey[i]);
                    ProperInterval interval = intervalConstraints.get(keyType);
                    if (interval!=null) {
                        sortKeyConstraints[i]=new EdgeSerializer.TypedInterval((InternalType) keyType,interval);
                        coveredTypes++;
                    }
                    if (interval==null || !interval.isPoint()) break;
                }
                boolean vertexConstraintApplies = sortKeyConstraints.length==0 ||
                        (sortKeyConstraints[sortKeyConstraints.length-1] != null && sortKeyConstraints[sortKeyConstraints.length-1].interval.isPoint());
                boolean hasSortKeyConstraint = sortKeyConstraints.length>0 && sortKeyConstraints[0]!=null;

                Direction[] dirs = {dir};
                if (dir == Direction.BOTH &&
                        (hasSortKeyConstraint || (vertexConstraintApplies && vertexConstraint != null))) {
                    //Split on direction in the presence of effective sort key constraints
                    dirs = new Direction[]{Direction.OUT, Direction.IN};
                }
                for (Direction dir : dirs) {
                    EdgeSerializer.VertexConstraint vertexCon = vertexConstraintApplies?vertexConstraint:null;
                    EdgeSerializer.TypedInterval[] sortConstraints = sortKeyConstraints;
                    if (bestCandidate.getMultiplicity().isUnique(dir)) {
                        vertexCon = null;
                        sortConstraints = new EdgeSerializer.TypedInterval[bestCandidate.getSortKey().length];
                    }

                    boolean isFitted = isIntervalFittedConditions && coveredTypes==intervalConstraints.size()
                            && vertexConstraint == vertexCon && sortConstraints == sortKeyConstraints;
                    SliceQuery q;
                    q = serializer.getQuery(bestCandidate, dir, sortConstraints, vertexCon);
                    q.setLimit(computeLimit(intervalConstraints.size()-coveredTypes, sliceLimit));
                    queries.add(new BackendQueryHolder<SliceQuery>(q, isFitted, true, null));
                }

            }
            if (queries.isEmpty())
                return BaseVertexCentricQuery.emptyQuery();

            conditions.add(getTypeCondition(ts));
        }

        return new BaseVertexCentricQuery(QueryUtil.simplifyQNF(conditions), dir, queries, limit);
    }

    private static boolean emptySortConstraints(EdgeSerializer.TypedInterval[] sortKeyConstraints) {
        return sortKeyConstraints.length==0 || sortKeyConstraints[0]==null;
    }


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

    private static final boolean isIntervalType(TitanType type) {
        return type.isPropertyKey() && Comparable.class.isAssignableFrom(((TitanKey) type).getDataType());
    }

    private static Condition<TitanRelation> getTypeCondition(Set<TitanType> types) {
        assert !types.isEmpty();
        if (types.size() == 1)
            return new RelationTypeCondition<TitanRelation>(types.iterator().next());

        Or<TitanRelation> typeCond = new Or<TitanRelation>(types.size());

        for (TitanType type : types)
            typeCond.add(new RelationTypeCondition<TitanRelation>(type));

        return typeCond;
    }

    private int computeLimit(int remainingConditions, int baseLimit) {
        if (baseLimit==Query.NO_LIMIT) return baseLimit;
        assert baseLimit>0;
        baseLimit = Math.max(baseLimit,Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, remainingConditions, baseLimit)));
        assert baseLimit>0;
        return baseLimit;
    }



}
