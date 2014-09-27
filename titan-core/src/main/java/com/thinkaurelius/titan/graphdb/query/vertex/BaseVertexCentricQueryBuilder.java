package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.schema.SchemaInspector;
import com.thinkaurelius.titan.graphdb.internal.*;
import com.thinkaurelius.titan.graphdb.query.*;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemRelationType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Builds a {@link com.thinkaurelius.titan.core.BaseVertexQuery}, optimizes the query and compiles the result into a {@link BaseVertexCentricQuery} which
 * is then executed by one of the extending classes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class BaseVertexCentricQueryBuilder<Q extends BaseVertexQuery<Q>> implements BaseVertexQuery<Q> {

    private static final String[] NO_TYPES = new String[0];
    private static final List<PredicateCondition<String, TitanRelation>> NO_CONSTRAINTS = ImmutableList.of();

    /**
     * The direction of this query. BOTH by default
     */
    protected Direction dir = Direction.BOTH;
    /**
     * The relation types (labels or keys) to query for. None by default which means query for any relation type.
     */
    protected String[] types = NO_TYPES;
    /**
     * The constraints added to this query. None by default.
     */
    protected List<PredicateCondition<String, TitanRelation>> constraints = NO_CONSTRAINTS;
    /**
     * The vertex to be used for the adjacent vertex constraint. If null, that means no such constraint. Null by default.
     */
    protected TitanVertex adjacentVertex = null;
    /**
     * The order in which the relations should be returned. None by default.
     */
    protected OrderList orders = new OrderList();
    /**
     * The limit of this query. No limit by default.
     */
    protected int limit = Query.NO_LIMIT;

    private final SchemaInspector schemaInspector;

    protected BaseVertexCentricQueryBuilder(SchemaInspector schemaInspector) {
        this.schemaInspector = schemaInspector;
    }

    protected abstract Q getThis();

    protected abstract TitanVertex getVertex(long vertexid);


    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */


    @Override
    public Q adjacent(TitanVertex vertex) {
        Preconditions.checkArgument(vertex!=null,"Not a valid vertex provided for adjacency constraint");
        this.adjacentVertex = vertex;
        return getThis();
    }

    private Q addConstraint(String type, TitanPredicate rel, Object value) {
        Preconditions.checkArgument(type!=null && StringUtils.isNotBlank(type) && rel!=null);
        //Treat special cases
        if (type.equals(ImplicitKey.ADJACENT_ID.getName())) {
            Preconditions.checkArgument(rel == Cmp.EQUAL,"Only equality constraints are supported for %s",type);
            Preconditions.checkArgument(value instanceof Number,"Expected valid vertex id: %s",value);
            return adjacent(getVertex(((Number)value).longValue()));
        } else if (type.equals(ImplicitKey.ID.getName())) {
            Preconditions.checkArgument(value instanceof RelationIdentifier,"Expected valid relation id: %s",value);
            return addConstraint(ImplicitKey.TITANID.getName(),rel,((RelationIdentifier)value).getRelationId());
        }
        if (constraints==NO_CONSTRAINTS) constraints = new ArrayList<PredicateCondition<String, TitanRelation>>(5);
        constraints.add(new PredicateCondition<String, TitanRelation>(type, rel, value));
        return getThis();
    }

    @Override
    public Q has(PropertyKey key, Object value) {
        return has(key.getName(), value);
    }

    @Override
    public Q has(EdgeLabel label, TitanVertex vertex) {
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
    public Q has(PropertyKey key, Predicate predicate, Object value) {
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
    public <T extends Comparable<?>> Q interval(PropertyKey key, T start, T end) {
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
    public Q types(RelationType... types) {
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

    public Q type(RelationType type) {
        return types(type.getName());
    }

    public Q types(String... types) {
        if (types==null) types = NO_TYPES;
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
        Preconditions.checkArgument(schemaInspector.containsPropertyKey(key),"Provided key does not exist: %s",key);
        return orderBy(schemaInspector.getPropertyKey(key), order);
    }

    @Override
    public Q orderBy(PropertyKey key, Order order) {
        Preconditions.checkArgument(key!=null && order!=null,"Need to specify and key and an order");
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.getDataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.getName(), key.getDataType());
        Preconditions.checkArgument(key.getCardinality()== Cardinality.SINGLE, "Ordering is undefined on multi-valued key [%s]", key.getName());
        Preconditions.checkArgument(!(key instanceof SystemRelationType),"Cannot use system types in ordering: %s",key);
        Preconditions.checkArgument(!orders.containsKey(key));
        Preconditions.checkArgument(orders.isEmpty(),"Only a single sort order is supported on vertex queries");
        orders.add(key, order);
        return getThis();
    }


    /* ---------------------------------------------------------------
     * Inspection Methods
	 * ---------------------------------------------------------------
	 */

    protected final boolean hasTypes() {
        return types.length>0;
    }

    protected final boolean hasSingleType() {
        return types.length==1 && schemaInspector.getRelationType(types[0])!=null;
    }

    protected final RelationType getSingleType() {
        Preconditions.checkArgument(hasSingleType());
        return schemaInspector.getRelationType(types[0]);
    }

    /**
     * Whether this query is asking for the value of an {@link com.thinkaurelius.titan.graphdb.types.system.ImplicitKey}.
     * </p>
     * Handling of implicit keys is completely distinct from "normal" query execution and handled extra
     * for completeness reasons.
     *
     * @param returnType
     * @return
     */
    protected final boolean isImplicitKeyQuery(RelationCategory returnType) {
        if (returnType==RelationCategory.EDGE || types.length!=1 || !constraints.isEmpty()) return false;
        return schemaInspector.getRelationType(types[0]) instanceof ImplicitKey;
    }


}
