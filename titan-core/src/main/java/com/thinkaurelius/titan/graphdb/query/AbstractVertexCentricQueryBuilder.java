package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Interval;
import com.thinkaurelius.titan.util.datastructures.PointInterval;
import com.thinkaurelius.titan.util.datastructures.ProperInterval;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

abstract class AbstractVertexCentricQueryBuilder implements BaseVertexQuery {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AbstractVertexCentricQueryBuilder.class);

    private static final String[] NO_TYPES = new String[0];
    private static final List<PredicateCondition<String, TitanRelation>> NO_CONSTRAINTS = ImmutableList.of();

    protected final StandardTitanTx tx;

    //Initial query configuration
    protected Direction dir = Direction.BOTH;
    protected String[] types = NO_TYPES;
    protected List<PredicateCondition<String, TitanRelation>> constraints = NO_CONSTRAINTS;

    protected boolean includeHidden = false;
    protected int limit = Query.NO_LIMIT;


    public AbstractVertexCentricQueryBuilder(final StandardTitanTx tx, final EdgeSerializer serializer) {
        assert serializer != null;
        assert tx != null;
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
        assert type != null;
        assert rel != null;
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
    public AbstractVertexCentricQueryBuilder types(TitanType... type) {
        for (TitanType t : type) type(t);
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder labels(String... labels) {
        types = labels;
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder keys(String... keys) {
        types = keys;
        return this;
    }

    public AbstractVertexCentricQueryBuilder type(TitanType type) {
        types = new String[]{type.getName()};
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder direction(Direction d) {
        assert d != null;
        dir = d;
        return this;
    }

    public AbstractVertexCentricQueryBuilder includeHidden() {
        includeHidden = true;
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder limit(int limit) {
        assert limit >= 0;
        this.limit = limit;
        return this;
    }

    /* ---------------------------------------------------------------
     * Utility Methods
	 * ---------------------------------------------------------------
	 */

    protected InternalType getType(String typeName) {
        TitanType t = tx.getType(typeName);
        if (t == null && !tx.getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return (InternalType) t;
    }

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

    protected BaseVertexCentricQuery constructQuery(RelationType returnType) {
        assert returnType != null;
        if (limit == 0)
            return BaseVertexCentricQuery.emptyQuery();

        //Prepare direction
        if (returnType == RelationType.PROPERTY) {
            if (dir == Direction.IN)
                return BaseVertexCentricQuery.emptyQuery();

            dir = Direction.OUT;
        }

        assert getVertexConstraint() == null || returnType == RelationType.EDGE;

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
                    ((dir == Direction.BOTH || (returnType == RelationType.PROPERTY && dir == Direction.OUT))
                            && !conditions.hasChildren() && includeHidden), true, null);
            if (sliceLimit!=Query.NO_LIMIT && sliceLimit<Integer.MAX_VALUE/3) {
                //If only one direction is queried, ask for twice the limit from backend since approximately half will be filtered
                if (dir != Direction.BOTH && (returnType == RelationType.EDGE || returnType == RelationType.RELATION))
                    sliceLimit *= 2;
                //on properties, add some for the hidden properties on a vertex
                if (!includeHidden && (returnType == RelationType.PROPERTY || returnType == RelationType.RELATION))
                    sliceLimit += 3;
            }
            query.getBackendQuery().setLimit(computeLimit(conditions,sliceLimit));
            queries = ImmutableList.of(query);
            //Add remaining conditions that only apply if no type is defined
            if (!includeHidden)
                conditions.add(new HiddenFilterCondition<TitanRelation>());

            conditions.add(returnType);
        } else {
            Set<TitanType> ts = new HashSet<TitanType>(types.length);
            queries = new ArrayList<BackendQueryHolder<SliceQuery>>(types.length + 4);

            for (String typeName : types) {
                InternalType type = getType(typeName);
                if (type != null && (includeHidden || !type.isHidden())) {
                    ts.add(type);
                    if (type.isPropertyKey()) {
                        if (returnType == RelationType.EDGE)
                            throw new IllegalArgumentException("Querying for edges but including a property key: " + type.getName());
                        returnType = RelationType.PROPERTY;
                    }
                    if (type.isEdgeLabel()) {
                        if (returnType == RelationType.PROPERTY)
                            throw new IllegalArgumentException("Querying for properties but including an edge label: " + type.getName());
                        returnType = RelationType.EDGE;
                    }
                    //Construct sort key constraints (if any, and if not direction==Both)
                    EdgeSerializer.TypedInterval[] sortKeyConstraints = new EdgeSerializer.TypedInterval[type.getSortKey().length];
                    And<TitanRelation> remainingConditions = conditions;
                    boolean vertexConstraintApplies = type.getSortKey().length == 0 || conditions.hasChildren();
                    if (type.getSortKey().length > 0 && conditions.hasChildren()) {
                        remainingConditions = conditions.clone();
                        sortKeyConstraints = compileSortKeyConstraints(type,tx,remainingConditions);
                        if (sortKeyConstraints==null) continue; //Constraints cannot be matched

                        Interval interval;
                        if (sortKeyConstraints[sortKeyConstraints.length-1] == null ||
                                (interval=sortKeyConstraints[sortKeyConstraints.length-1].interval) == null || !interval.isPoint()) {
                            vertexConstraintApplies = false;

                        }
                    }
                    Direction[] dirs = {dir};
                    EdgeSerializer.VertexConstraint vertexConstraint = getVertexConstraint();
                    if (dir == Direction.BOTH &&
                            (hasSortKeyConstraints(sortKeyConstraints) || (vertexConstraintApplies && vertexConstraint != null))) {
                        //Split on direction in the presence of effective sort key constraints
                        dirs = new Direction[]{Direction.OUT, Direction.IN};
                    }
                    for (Direction dir : dirs) {
                        EdgeSerializer.VertexConstraint vertexCon = vertexConstraint;
                        if (vertexCon == null || !vertexConstraintApplies || type.isUnique(dir)) vertexCon = null;
                        EdgeSerializer.TypedInterval[] sortConstraints = sortKeyConstraints;
                        if (hasSortKeyConstraints(sortKeyConstraints) && type.isUnique(dir)) {
                            sortConstraints = new EdgeSerializer.TypedInterval[type.getSortKey().length];
                        }

                        boolean isFitted = !remainingConditions.hasChildren()
                                && vertexConstraint == vertexCon && sortConstraints == sortKeyConstraints;
                        SliceQuery q = serializer.getQuery(type, dir, sortConstraints, vertexCon);
                        q.setLimit(computeLimit(remainingConditions, sliceLimit));
                        queries.add(new BackendQueryHolder<SliceQuery>(q, isFitted, true, null));
                    }
                }
            }
            if (queries.isEmpty())
                return BaseVertexCentricQuery.emptyQuery();

            conditions.add(getTypeCondition(ts));
        }

        return new BaseVertexCentricQuery(QueryUtil.simplifyQNF(conditions), dir, queries, limit);
    }

    public EdgeSerializer.TypedInterval[] getFittingKeyConstraints(InternalType type) {
        if (constraints.isEmpty()) return new EdgeSerializer.TypedInterval[type.getSortKey().length];
        else if (dir==Direction.BOTH || type.isUnique(dir) || type.getSortKey().length<=0) return null;

        And<TitanRelation> conditions = QueryUtil.constraints2QNF(tx, constraints);
        if (conditions == null) return null;
        EdgeSerializer.TypedInterval[] sortKeyConstraints = compileSortKeyConstraints(type,tx,conditions);
        if (!conditions.isEmpty()) return null;
        return sortKeyConstraints;
    }

    private static EdgeSerializer.TypedInterval[] compileSortKeyConstraints(InternalType type, StandardTitanTx tx, And<TitanRelation> conditions) {
        long[] sortKeys = type.getSortKey();
        EdgeSerializer.TypedInterval[] sortKeyConstraints = new EdgeSerializer.TypedInterval[type.getSortKey().length];

        for (int i = 0; i < sortKeys.length; i++) {
            InternalType pktype = (InternalType) tx.getExistingType(sortKeys[i]);
            Interval interval = null;
            //First check for equality constraints, since those are the most constraining
            for (Iterator<Condition<TitanRelation>> iter = conditions.iterator(); iter.hasNext(); ) {
                PredicateCondition<TitanType, TitanRelation> atom = (PredicateCondition) iter.next();
                if (atom.getKey().equals(pktype) && atom.getPredicate() == Cmp.EQUAL && interval == null) {
                    interval = new PointInterval(atom.getValue());
                    iter.remove();
                }
            }

            //If there are no equality constraints, check if the sort key's datatype allows comparison
            //and if so, find a bounding interval from the remaining constraints
            if (interval == null && pktype.isPropertyKey()
                    && Comparable.class.isAssignableFrom(((TitanKey) pktype).getDataType())) {
                ProperInterval pint = new ProperInterval();
                for (Iterator<Condition<TitanRelation>> iter = conditions.iterator(); iter.hasNext(); ) {
                    Condition<TitanRelation> cond = iter.next();
                    if (cond instanceof PredicateCondition) {
                        PredicateCondition<TitanType, TitanRelation> atom = (PredicateCondition) cond;
                        if (atom.getKey().equals(pktype)) {
                            TitanPredicate predicate = atom.getPredicate();
                            Object value = atom.getValue();
                            if (predicate instanceof Cmp) {
                                switch ((Cmp) predicate) {
                                    case NOT_EQUAL:
                                        break;
                                    case LESS_THAN:
                                        if (pint.getEnd() == null || pint.getEnd().compareTo(value) >= 0) {
                                            pint.setEnd((Comparable) value);
                                            pint.setEndInclusive(false);
                                        }
                                        iter.remove();
                                        break;
                                    case LESS_THAN_EQUAL:
                                        if (pint.getEnd() == null || pint.getEnd().compareTo(value) > 0) {
                                            pint.setEnd((Comparable) value);
                                            pint.setEndInclusive(true);
                                        }
                                        iter.remove();
                                        break;
                                    case GREATER_THAN:
                                        if (pint.getStart() == null || pint.getStart().compareTo(value) <= 0) {
                                            pint.setStart((Comparable) value);
                                            pint.setStartInclusive(false);
                                        }
                                        iter.remove();
                                        break;
                                    case GREATER_THAN_EQUAL:
                                        if (pint.getStart() == null || pint.getStart().compareTo(value) < 0) {
                                            pint.setStart((Comparable) value);
                                            pint.setStartInclusive(true);
                                        }
                                        iter.remove();
                                        break;
                                }
                            }
                        }
                    } else if (cond instanceof Or) {
                        //Grab a probe so we can investigate what type of or-condition this is and whether it allows us to constrain this sort key
                        Condition probe = ((Or) cond).get(0);
                        if (probe instanceof PredicateCondition && ((PredicateCondition) probe).getKey().equals(pktype) &&
                                ((PredicateCondition) probe).getPredicate() == Cmp.EQUAL) {
                            //We make the assumption that this or-condition is a group of equality constraints for the same type (i.e. an unrolled Contain.IN)
                            //This assumption is enforced by precondition statements below
                            //TODO: Consider splitting query on sort key with a limited number (<=3) of possible values in or-clause

                            //Now, we find the smallest and largest value in this group of equality constraints to bound the interval
                            Comparable smallest = null, largest = null;
                            for (Condition child : cond.getChildren()) {
                                assert child instanceof PredicateCondition;
                                PredicateCondition pc = (PredicateCondition) child;
                                assert pc.getKey().equals(pktype);
                                assert pc.getPredicate() == Cmp.EQUAL;

                                Object v = pc.getValue();
                                if (smallest == null) {
                                    smallest = (Comparable) v;
                                    largest = (Comparable) v;
                                } else {
                                    if (smallest.compareTo(v) > 0) {
                                        smallest = (Comparable) v;
                                    } else if (largest.compareTo(v) < 0) {
                                        largest = (Comparable) v;
                                    }
                                }
                            }
                            //After finding the smallest and largest value respectively, we constrain the interval
                            assert smallest != null && largest != null; //due to probing, there must be at least one
                            if (pint.getEnd() == null || pint.getEnd().compareTo(largest) > 0) {
                                pint.setEnd(largest);
                                pint.setEndInclusive(true);
                            }
                            if (pint.getStart() == null || pint.getStart().compareTo(smallest) < 0) {
                                pint.setStart(smallest);
                                pint.setStartInclusive(true);
                            }
                            //We cannot remove this condition from remainingConditions, since its not exactly fulfilled (only bounded)
                        }
                    }
                }
                if (pint.isEmpty()) return null;
                if (pint.getStart() != null || pint.getEnd() != null) interval = pint;
            }

            sortKeyConstraints[i] = new EdgeSerializer.TypedInterval(pktype, interval);
            if (interval == null || !interval.isPoint()) {
                break;
            }
        }
        return sortKeyConstraints;
    }

    public static boolean hasSortKeyConstraints(EdgeSerializer.TypedInterval[] cons) {
        return cons.length > 0 && cons[0] != null;
    }

    private static Condition<TitanRelation> getTypeCondition(Set<TitanType> types) {
        assert !types.isEmpty();
        if (types.size() == 1)
            return new LabelCondition<TitanRelation>(types.iterator().next());

        Or<TitanRelation> typeCond = new Or<TitanRelation>(types.size());

        for (TitanType type : types)
            typeCond.add(new LabelCondition<TitanRelation>(type));

        return typeCond;
    }

    private int computeLimit(And<TitanRelation> conditions, int baseLimit) {
        if (baseLimit==Query.NO_LIMIT) return baseLimit;
        assert baseLimit>0;
        baseLimit = Math.max(baseLimit,Math.min(HARD_MAX_LIMIT, QueryUtil.adjustLimitForTxModifications(tx, conditions.size(), baseLimit)));
        assert baseLimit>0;
        return baseLimit;
    }


}
