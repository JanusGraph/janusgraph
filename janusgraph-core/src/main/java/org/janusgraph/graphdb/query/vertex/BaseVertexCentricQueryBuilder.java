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

package org.janusgraph.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.SchemaInspector;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.ElementUtils;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.types.system.SystemRelationType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds a {@link org.janusgraph.core.BaseVertexQuery}, optimizes the query and compiles the result into a {@link BaseVertexCentricQuery} which
 * is then executed by one of the extending classes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class BaseVertexCentricQueryBuilder<Q extends BaseVertexQuery<Q>> implements BaseVertexQuery<Q> {

    private static final String[] NO_TYPES = new String[0];
    private static final List<PredicateCondition<String, JanusGraphRelation>> NO_CONSTRAINTS = Collections.emptyList();

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
    protected List<PredicateCondition<String, JanusGraphRelation>> constraints = NO_CONSTRAINTS;
    /**
     * The vertex to be used for the adjacent vertex constraint. If null, that means no such constraint. Null by default.
     */
    protected JanusGraphVertex adjacentVertex = null;
    /**
     * The order in which the relations should be returned. None by default.
     */
    protected final OrderList orders = new OrderList();
    /**
     * The limit of this query. No limit by default.
     */
    protected int limit = Query.NO_LIMIT;

    private final SchemaInspector schemaInspector;

    protected BaseVertexCentricQueryBuilder(SchemaInspector schemaInspector) {
        this.schemaInspector = schemaInspector;
    }

    protected abstract Q getThis();

    protected abstract JanusGraphVertex getVertex(long vertexId);


    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */


    @Override
    public Q adjacent(Vertex vertex) {
        Preconditions.checkArgument(vertex instanceof JanusGraphVertex, "Not a valid vertex provided for adjacency constraint");
        this.adjacentVertex = (JanusGraphVertex) vertex;
        return getThis();
    }

    private Q addConstraint(String type, JanusGraphPredicate rel, Object value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(type));
        Preconditions.checkNotNull(rel);
        //Treat special cases
        if (type.equals(ImplicitKey.ADJACENT_ID.name())) {
            Preconditions.checkArgument(rel == Cmp.EQUAL, "Only equality constraints are supported for %s", type);
            long vertexId = ElementUtils.getVertexId(value);
            Preconditions.checkArgument(vertexId > 0, "Expected valid vertex id: %s", value);
            return adjacent(getVertex(vertexId));
        } else if (type.equals(ImplicitKey.ID.name())) {
            RelationIdentifier rid = ElementUtils.getEdgeId(value);
            Preconditions.checkNotNull(rid, "Expected valid relation id: %s", value);
            return addConstraint(ImplicitKey.JANUSGRAPHID.name(), rel, rid.getRelationId());
        } else {
            Preconditions.checkArgument(rel.isValidCondition(value), "Invalid condition provided: %s", value);
        }
        if (constraints == NO_CONSTRAINTS) constraints = new ArrayList<>(5);
        constraints.add(new PredicateCondition<>(type, rel, value));
        return getThis();
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
    public Q has(String key) {
        return has(key, Cmp.NOT_EQUAL, null);
    }

    @Override
    public Q hasNot(String key) {
        return has(key, Cmp.EQUAL, null);
    }

    @Override
    public Q has(String key, JanusGraphPredicate predicate, Object value) {
        return addConstraint(key, predicate, value);
    }

    @Override
    public <T extends Comparable<?>> Q interval(String key, T start, T end) {
        addConstraint(key, Cmp.GREATER_THAN_EQUAL, start);
        return addConstraint(key, Cmp.LESS_THAN, end);
    }

    @Override
    public Q types(RelationType... types) {
        String[] ts = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            ts[i] = types[i].name();
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
        return types(type.name());
    }

    @Override
    public Q types(String... types) {
        if (types == null) types = NO_TYPES;
        for (String type : types) Preconditions.checkArgument(StringUtils.isNotBlank(type), "Invalid type: %s", type);
        this.types = types;
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
    public Q orderBy(String keyName, org.apache.tinkerpop.gremlin.process.traversal.Order order) {
        Preconditions.checkArgument(schemaInspector.containsPropertyKey(keyName), "Provided key does not exist: %s", keyName);
        PropertyKey key = schemaInspector.getPropertyKey(keyName);
        Preconditions.checkArgument(key != null && order != null, "Need to specify and key and an order");
        Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.dataType()),
                "Can only order on keys with comparable data type. [%s] has datatype [%s]", key.name(), key.dataType());
        Preconditions.checkArgument(!(key instanceof SystemRelationType), "Cannot use system types in ordering: %s", key);
        Preconditions.checkArgument(!orders.containsKey(key));
        Preconditions.checkArgument(orders.isEmpty(), "Only a single sort order is supported on vertex queries");
        orders.add(key, Order.convert(order));
        return getThis();
    }


    /* ---------------------------------------------------------------
     * Inspection Methods
	 * ---------------------------------------------------------------
	 */

    protected final boolean hasTypes() {
        return types.length > 0;
    }

    protected final boolean hasSingleType() {
        return types.length == 1 && schemaInspector.getRelationType(types[0]) != null;
    }

    protected final RelationType getSingleType() {
        Preconditions.checkArgument(hasSingleType());
        return schemaInspector.getRelationType(types[0]);
    }

    /**
     * Whether this query is asking for the value of an {@link org.janusgraph.graphdb.types.system.ImplicitKey}.
     * <p>
     * Handling of implicit keys is completely distinct from "normal" query execution and handled extra
     * for completeness reasons.
     *
     * @param returnType
     * @return
     */
    protected final boolean isImplicitKeyQuery(RelationCategory returnType) {
        return returnType != RelationCategory.EDGE && types.length == 1 && constraints.isEmpty() && schemaInspector.getRelationType(types[0]) instanceof ImplicitKey;
    }


}
