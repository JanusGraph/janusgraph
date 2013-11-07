package com.thinkaurelius.titan.graphdb.query;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.DirectionCondition;
import com.thinkaurelius.titan.graphdb.query.condition.IncidenceCondition;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertexCentricQueryBuilder extends AbstractVertexCentricQueryBuilder implements TitanVertexQuery {

    private static final Logger log = LoggerFactory.getLogger(VertexCentricQueryBuilder.class);

    private final InternalVertex vertex;

    //Additional constraints
    private TitanVertex adjacentVertex = null;

    public VertexCentricQueryBuilder(InternalVertex v, EdgeSerializer serializer) {
        super(v.tx(), serializer);
        Preconditions.checkNotNull(v);
        this.vertex = v;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    @Override
    public TitanVertexQuery adjacentVertex(TitanVertex vertex) {
        Preconditions.checkNotNull(vertex);
        this.adjacentVertex = vertex;
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(TitanKey key, Object value) {
        super.has(key, value);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(TitanLabel label, TitanVertex vertex) {
        super.has(label, vertex);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(String type, Object value) {
        super.has(type, value);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder hasNot(String key, Object value) {
        super.hasNot(key, value);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(String key, Predicate predicate, Object value) {
        super.has(key, predicate, value);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(TitanKey key, Predicate predicate, Object value) {
        super.has(key, predicate, value);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder has(String key) {
        super.has(key);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder hasNot(String key) {
        super.hasNot(key);
        return this;
    }

    @Override
    public <T extends Comparable<?>> VertexCentricQueryBuilder interval(TitanKey key, T start, T end) {
        super.interval(key, start, end);
        return this;
    }

    @Override
    public <T extends Comparable<?>> VertexCentricQueryBuilder interval(String key, T start, T end) {
        super.interval(key, start, end);
        return this;
    }

    @Override
    @Deprecated
    public <T extends Comparable<T>> VertexCentricQueryBuilder has(String key, T value, Compare compare) {
        super.has(key, value, compare);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder types(TitanType... types) {
        super.types(types);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder labels(String... labels) {
        super.labels(labels);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder keys(String... keys) {
        super.keys(keys);
        return this;
    }

    public VertexCentricQueryBuilder type(TitanType type) {
        super.type(type);
        return this;
    }

    public VertexCentricQueryBuilder type(String type) {
        super.type(type);
        return this;
    }

    @Override
    public VertexCentricQueryBuilder direction(Direction d) {
        super.direction(d);
        return this;
    }

    public VertexCentricQueryBuilder includeHidden() {
        super.includeHidden();
        return this;
    }

    @Override
    public VertexCentricQueryBuilder limit(int limit) {
        super.limit(limit);
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    @Override
    protected EdgeSerializer.VertexConstraint getVertexConstraint() {
        if (adjacentVertex != null && vertex.hasId() && adjacentVertex.hasId()) {
            return new EdgeSerializer.VertexConstraint(vertex.getID(), adjacentVertex.getID());
        } else return null;
    }

    public VertexCentricQuery constructQuery(RelationType returnType) {
        BaseVertexCentricQuery vq = super.constructQuery(returnType);
        Condition<TitanRelation> condition = vq.getCondition();
        if (!vq.isEmpty()) {
            //Add other-vertex and direction related conditions
            And<TitanRelation> newcond = (condition instanceof And) ? (And) condition : new And<TitanRelation>(condition);
            newcond.add(new DirectionCondition<TitanRelation>(vertex,getDirection()));
            if (adjacentVertex != null)
                newcond.add(new IncidenceCondition<TitanRelation>(vertex,adjacentVertex));
            condition=newcond;
        }
        if (returnType == RelationType.PROPERTY && hasTypes() && tx.getConfiguration().hasPropertyPrefetching()) {
            //Retrieve all
            vertex.query().includeHidden().properties().iterator().hasNext();
        }
        return new VertexCentricQuery(vertex, condition, vq.getDirection(), vq.getQueries(), vq.getLimit());
    }

    public Iterable<TitanRelation> relations(RelationType returnType) {
        return new QueryProcessor<VertexCentricQuery,TitanRelation,SliceQuery>(constructQuery(returnType), tx.edgeProcessor);
    }


    @Override
    public Iterable<TitanEdge> titanEdges() {
        return (Iterable) relations(RelationType.EDGE);
    }


    @Override
    public Iterable<TitanProperty> properties() {
        return (Iterable) relations(RelationType.PROPERTY);
    }

    @Override
    public Iterable<TitanRelation> relations() {
        return relations(RelationType.RELATION);
    }

    @Override
    public Iterable<Edge> edges() {
        return (Iterable) titanEdges();
    }

    @Override
    public long count() {
        return Iterables.size(titanEdges());
    }

    @Override
    public long propertyCount() {
        return Iterables.size(properties());
    }

    @Override
    public Iterable<Vertex> vertices() {
        return (Iterable) edges2Vertices(titanEdges(), vertex);
    }

    @Override
    public VertexList vertexIds() {
        return edges2VertexIds(titanEdges(), vertex);
    }


}
