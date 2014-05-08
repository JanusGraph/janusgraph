package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.QueryProcessor;
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
    private boolean queryOnlyLoaded = false;

    public VertexCentricQueryBuilder(InternalVertex v) {
        super(v.tx());
        Preconditions.checkNotNull(v);
        Preconditions.checkArgument(!v.isRemoved(),"Cannot access a removed vertex: %s",v);
        this.vertex = v;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    @Override
    public VertexCentricQueryBuilder adjacentVertex(TitanVertex vertex) {
        Preconditions.checkNotNull(vertex);
        this.adjacentVertex = vertex;
        return this;
    }

    public VertexCentricQueryBuilder queryOnlyLoaded() {
        queryOnlyLoaded=true;
        return this;
    }

    /*
    ########### SIMPLE OVERWRITES ##########
	 */

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

    @Override
    public VertexCentricQueryBuilder direction(Direction d) {
        super.direction(d);
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

    public VertexCentricQuery constructQuery(RelationCategory returnType) {
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
        VertexCentricQuery query = new VertexCentricQuery(vertex, condition, vq.getDirection(), vq.getQueries(), vq.getLimit());
        Preconditions.checkArgument(!queryOnlyLoaded || query.isSimple(),"Query-only-loaded only works on simple queries");
        return query;
    }

    //#### RELATIONS

    private Iterable<TitanRelation> relations(RelationCategory returnType) {
        if (isImplicitKeyQuery(returnType)) return executeImplicitKeyQuery(vertex);
        return relations(constructQuery(returnType));
    }

    private Iterable<TitanRelation> relations(VertexCentricQuery query) {
        if (useSimpleQueryProcessor(query)) return new SimpleVertexQueryProcessor(query,tx).relations();
        else return new QueryProcessor<VertexCentricQuery,TitanRelation,SliceQuery>(query, tx.edgeProcessor);
    }

    private boolean useSimpleQueryProcessor(VertexCentricQuery query) {
        return query.isSimple() && (vertex.isLoaded() || queryOnlyLoaded);
    }

    @Override
    public Iterable<TitanEdge> titanEdges() {
        return (Iterable) relations(RelationCategory.EDGE);
    }

    @Override
    public Iterable<TitanProperty> properties() {
        return (Iterable) relations(RelationCategory.PROPERTY);
    }

    @Override
    public Iterable<TitanRelation> relations() {
        return relations(RelationCategory.RELATION);
    }

    @Override
    public Iterable<Edge> edges() {
        return (Iterable) titanEdges();
    }

    //#### VERTICES

    @Override
    public Iterable<Vertex> vertices() {
        VertexCentricQuery query = constructQuery(RelationCategory.EDGE);
        if (useSimpleQueryProcessor(query)) return (Iterable)new SimpleVertexQueryProcessor(query,tx).vertexIds();
        else return (Iterable) edges2Vertices((Iterable)relations(query), vertex);
    }

    @Override
    public VertexList vertexIds() {
        VertexCentricQuery query = constructQuery(RelationCategory.EDGE);
        if (useSimpleQueryProcessor(query)) return new SimpleVertexQueryProcessor(query,tx).vertexIds();
        return edges2VertexIds((Iterable)relations(query), vertex);
    }

    //#### COUNTS

    @Override
    public long count() {
        VertexCentricQuery query = constructQuery(RelationCategory.EDGE);
        if (useSimpleQueryProcessor(query)) return new SimpleVertexQueryProcessor(query,tx).vertexIds().size();
        else return Iterables.size(relations(query));
    }

    @Override
    public long propertyCount() {
        return Iterables.size(properties());
    }
}
