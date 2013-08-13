package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
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

import java.util.*;

abstract class AbstractVertexCentricQueryBuilder implements TitanVertexQuery {

    private static final Logger log = LoggerFactory.getLogger(AbstractVertexCentricQueryBuilder.class);

    private final EdgeSerializer serializer;

    private Direction dir;
    private Set<String> types;
    private List<PredicateCondition<String,TitanRelation>> constraints;

    private boolean includeHidden;
    private int limit = Query.NO_LIMIT;


    public AbstractVertexCentricQueryBuilder(final EdgeSerializer serializer) {
        Preconditions.checkNotNull(serializer);
        this.serializer=serializer;
        //Initial query configuration
        dir = Direction.BOTH;
        types = new HashSet<String>(4);
        constraints = new ArrayList(6);
        includeHidden = false;
    }

    Direction getDirection() {
        return dir;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    abstract StandardTitanTx getTx();

    private final InternalType getType(String typeName) {
        TitanType t = getTx().getType(typeName);
        if (t == null && !getTx().getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return (InternalType)t;
    }

    private AbstractVertexCentricQueryBuilder addConstraint(String type, TitanPredicate rel, Object value) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(rel);
        constraints.add(new PredicateCondition<String,TitanRelation>(type, rel, value));
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder has(TitanKey key, Object value) {
        return has(key.getName(),value);
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
    public TitanVertexQuery hasNot(String key, Object value) {
        return has(key, Cmp.NOT_EQUAL, value);
    }

    @Override
    public TitanVertexQuery has(String key, Predicate predicate, Object value) {
        return addConstraint(key, TitanPredicate.Converter.convert(predicate), value);
    }

    @Override
    public TitanVertexQuery has(TitanKey key, Predicate predicate, Object value) {
        return has(key.getName(), predicate, value);
    }

    @Override
    public TitanVertexQuery has(String key) {
        return has(key,Cmp.NOT_EQUAL,(Object)null);
    }

    @Override
    public TitanVertexQuery hasNot(String key) {
        return has(key,Cmp.EQUAL,(Object)null);
    }

    @Override
    public <T extends Comparable<?>> AbstractVertexCentricQueryBuilder interval(TitanKey key, T start, T end) {
        return interval(key.getName(), start, end);
    }

    @Override
    public <T extends Comparable<?>> AbstractVertexCentricQueryBuilder interval(String key, T start, T end) {
        addConstraint(key,Cmp.GREATER_THAN_EQUAL,start);
        return addConstraint(key, Cmp.LESS_THAN, end);
    }

    @Override
    @Deprecated
    public <T extends Comparable<T>> AbstractVertexCentricQueryBuilder has(String key, T value, Compare compare) {
        return addConstraint(key,TitanPredicate.Converter.convert(compare),value);
    }

    @Override
    public AbstractVertexCentricQueryBuilder types(TitanType... type) {
        for (TitanType t : type) type(t);
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder labels(String... labels) {
        types.addAll(Arrays.asList(labels));
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder keys(String... keys) {
        types.addAll(Arrays.asList(keys));
        return this;
    }

    public AbstractVertexCentricQueryBuilder type(TitanType type) {
        return type(type.getName());
    }

    public AbstractVertexCentricQueryBuilder type(String type) {
        types.add(type);
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder direction(Direction d) {
        Preconditions.checkNotNull(d);
        dir = d;
        return this;
    }

    public AbstractVertexCentricQueryBuilder includeHidden() {
        includeHidden = true;
        return this;
    }

    @Override
    public AbstractVertexCentricQueryBuilder limit(int limit) {
        Preconditions.checkArgument(limit>=0,"Limit must be non-negative [%s]",limit);
        this.limit = limit;
        return this;
    }


    /* ---------------------------------------------------------------
     * Query Optimization
	 * ---------------------------------------------------------------
	 */

    protected EdgeSerializer.VertexConstraint getVertexConstraint() {
        return null;
    }

    private static final int DEFAULT_NO_LIMIT = 100;
    private static final int MAX_BASE_LIMIT = 20000;
    private static final int HARD_MAX_LIMIT = 50000;

    protected BaseVertexCentricQuery constructQuery(RelationType returnType) {
        Preconditions.checkNotNull(returnType);
        if (limit==0) return BaseVertexCentricQuery.emptyQuery();

        //Prepare direction
        if (returnType== RelationType.PROPERTY) {
            if (dir==Direction.IN) return BaseVertexCentricQuery.emptyQuery();
            else dir=Direction.OUT;
        }
        Preconditions.checkArgument(getVertexConstraint()==null || returnType==RelationType.EDGE);

        //Prepare constraints
        And<TitanRelation> conditions = new And<TitanRelation>(constraints.size()+4);
        if (!QueryUtil.prepareConstraints(getTx(),conditions,constraints)) return BaseVertexCentricQuery.emptyQuery();

        Preconditions.checkArgument(limit>0);
        int sliceLimit = limit;
        if (sliceLimit==Query.NO_LIMIT) sliceLimit=DEFAULT_NO_LIMIT;
        else sliceLimit=Math.min(sliceLimit,MAX_BASE_LIMIT);

        //Construct (optimal) SliceQueries
        List<BackendQueryHolder<SliceQuery>> queries=null;
        if (types.isEmpty()) {
            BackendQueryHolder<SliceQuery> query = new BackendQueryHolder<SliceQuery>(serializer.getQuery(returnType),
                            ((dir==Direction.BOTH || returnType==RelationType.PROPERTY && dir==Direction.OUT)
                                          && !conditions.hasChildren() && includeHidden),null);
            query.getBackendQuery().setLimit(computeLimit(conditions,
                    ((dir != Direction.BOTH && (returnType == RelationType.EDGE || returnType == RelationType.RELATION)) ? sliceLimit * 2 : sliceLimit) +
                            //If only one direction is queried, ask for twice the limit from backend since approximately half will be filtered
                            ((!includeHidden && (returnType == RelationType.PROPERTY || returnType == RelationType.RELATION)) ? 2 : 0)
                    //on properties, add some for the hidden properties on a vertex
            ));
            queries.add(query);
            //Add remaining conditions that only apply if no type is defined
            if (!includeHidden) conditions.add(new HiddenFilterCondition<TitanRelation>());
            conditions.add(returnType);
        } else {
            Set<TitanType> ts = new HashSet<TitanType>(types.size());
            queries = new ArrayList<BackendQueryHolder<SliceQuery>>(types.size()+4);

            for (String typeName : types) {
                InternalType type = getType(typeName);
                if (type!=null && (includeHidden || !type.isHidden())) {
                    ts.add(type);
                    if (type.isPropertyKey()) {
                        if (returnType==RelationType.EDGE)
                            throw new IllegalArgumentException("Querying for edges but including a property key: " + type.getName());
                        returnType=RelationType.PROPERTY;
                    }
                    if (type.isEdgeLabel()) {
                        if (returnType== RelationType.PROPERTY)
                            throw new IllegalArgumentException("Querying for properties but including an edge label: " + type.getName());
                        returnType = RelationType.EDGE;
                    }
                    //Construct primary key constraints (if any, and if not direction==Both)
                    EdgeSerializer.TypedInterval[] primaryKeyConstraints;
                    And<TitanRelation> remainingConditions=conditions;
                    boolean vertexConstraintApplies = true;
                    if (type.getPrimaryKey().length>0 && conditions.hasChildren()) {
                        remainingConditions=conditions.clone();
                        long[] primaryKeys = type.getPrimaryKey();
                        primaryKeyConstraints=new EdgeSerializer.TypedInterval[primaryKeys.length];

                        for (int i=0;i<primaryKeys.length;i++) {
                            TitanType pktype = getTx().getExistingType(primaryKeys[i]);
                            Interval interval=null;
                            //First check for equality constraints
                            for (Iterator<Condition<TitanRelation>> iter=remainingConditions.iterator();iter.hasNext();) {
                                PredicateCondition<TitanType,TitanRelation> atom = (PredicateCondition)iter.next();
                                if (atom.getKey().equals(pktype) && atom.getPredicate()==Cmp.EQUAL && interval==null) {
                                    interval=new PointInterval(atom.getCondition());
                                    iter.remove();
                                }
                            }

                            if (interval==null && pktype.isPropertyKey()
                                    && Comparable.class.isAssignableFrom(((TitanKey) pktype).getDataType())) {
                                ProperInterval pint = new ProperInterval();
                                for (Iterator<Condition<TitanRelation>> iter=remainingConditions.iterator();iter.hasNext();) {
                                    PredicateCondition<TitanType,TitanRelation> atom = (PredicateCondition)iter.next();
                                    if (atom.getKey().equals(pktype)) {
                                        TitanPredicate predicate = atom.getPredicate();
                                        Object value = atom.getCondition();
                                        if (predicate instanceof Cmp) {
                                            switch ((Cmp)predicate) {
                                                case NOT_EQUAL: break;
                                                case LESS_THAN:
                                                    if (pint.getEnd()==null || pint.getEnd().compareTo(value)<=0) {
                                                        pint.setEnd((Comparable)value);
                                                        pint.setEndInclusive(false);
                                                    }
                                                    iter.remove();
                                                    break;
                                                case LESS_THAN_EQUAL:
                                                    if (pint.getEnd()==null || pint.getEnd().compareTo(value)<0) {
                                                        pint.setEnd((Comparable)value);
                                                        pint.setEndInclusive(true);
                                                    }
                                                    iter.remove();
                                                    break;
                                                case GREATER_THAN:
                                                    if (pint.getStart()==null || pint.getStart().compareTo(value)>=0) {
                                                        pint.setStart((Comparable)value);
                                                        pint.setStartInclusive(false);
                                                    }
                                                    iter.remove();
                                                    break;
                                                case GREATER_THAN_EQUAL:
                                                    if (pint.getStart()==null || pint.getStart().compareTo(value)>0) {
                                                        pint.setStart((Comparable)value);
                                                        pint.setStartInclusive(true);
                                                    }
                                                    iter.remove();
                                                    break;
                                            }
                                        } else if (predicate== Contain.IN) {
                                            //TODO: Consider splitting query for IN constraints on primary key with a limited number (<=3) of possible values
                                            Comparable smallest = null, largest = null;
                                            for (Object v: ((Collection)value)) {
                                                if (smallest==null) {
                                                    smallest= (Comparable) v; largest= (Comparable) v;
                                                } else {
                                                    if (smallest.compareTo(v)>0) {
                                                        smallest= (Comparable) v;
                                                    } else if (largest.compareTo(v)<0) {
                                                        largest = (Comparable)v;
                                                    }
                                                }
                                            }
                                            Preconditions.checkArgument(smallest!=null && largest!=null);
                                            if (pint.getEnd()==null || pint.getEnd().compareTo(largest)<0) {
                                                pint.setEnd(largest);
                                                pint.setEndInclusive(true);
                                            }
                                            if (pint.getStart()==null || pint.getStart().compareTo(smallest)>0) {
                                                pint.setStart(smallest);
                                                pint.setStartInclusive(true);
                                            }
                                        }


                                    }
                                }
                                if (pint.isEmpty()) return BaseVertexCentricQuery.emptyQuery();
                                if (pint.getStart()!=null || pint.getEnd()!=null) interval=pint;
                            }

                            primaryKeyConstraints[i]=new EdgeSerializer.TypedInterval(type,interval);
                            if (interval==null || !interval.isPoint()) {
                                vertexConstraintApplies=false;
                                break;
                            }
                        }
                    } else primaryKeyConstraints= new EdgeSerializer.TypedInterval[0];

                    Direction[] dirs = {dir};
                    EdgeSerializer.VertexConstraint vertexConstraint = getVertexConstraint();
                    if (dir==Direction.BOTH &&
                            ((primaryKeyConstraints.length>0 && primaryKeyConstraints[0]!=null) || (vertexConstraintApplies && vertexConstraint!=null)) ) {
                        //Split on direction in the presence of effective primary key constraints
                        dirs = new Direction[]{Direction.OUT,Direction.IN};
                    }
                    for (Direction dir : dirs) {
                        EdgeSerializer.VertexConstraint vertexCon = vertexConstraint;
                        if (!vertexConstraintApplies || type.isUnique(dir)) vertexCon=null;
                        SliceQuery q=serializer.getQuery(type,dir,primaryKeyConstraints,vertexCon);
                        q.setLimit(computeLimit(remainingConditions, sliceLimit));
                        queries.add(new BackendQueryHolder<SliceQuery>(q,(!remainingConditions.hasChildren() && vertexConstraint==vertexCon),null));
                    }
                }
            }
            if (queries.isEmpty()) return BaseVertexCentricQuery.emptyQuery();
            else conditions.add(new LabelCondition<TitanRelation>(ts));
        }

        return new BaseVertexCentricQuery(conditions,queries,limit);
    }

    private final int computeLimit(And<TitanRelation> conditions, int baseLimit) {
        return getVertexConstraint()!=null?HARD_MAX_LIMIT:   //a vertex constraint is so selective, that we likely have to retrieve all edges
                Math.min(HARD_MAX_LIMIT,QueryUtil.adjustLimitForTxModifications(getTx(),conditions,baseLimit));
    }


}
