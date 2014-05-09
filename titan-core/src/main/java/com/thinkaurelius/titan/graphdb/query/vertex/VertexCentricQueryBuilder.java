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

/**
 * Implementation of {@link TitanVertexQuery} that extends {@link AbstractVertexCentricQueryBuilder}
 * for all the query building and optimization and adds only the execution logic in
 * {@link #constructQuery(com.thinkaurelius.titan.graphdb.internal.RelationCategory)}. However, there is
 * one important special case: If the constructed query is simple (i.e. {@link com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQuery#isSimple()=true}
 * then we use the {@link SimpleVertexQueryProcessor} to execute the query instead of the generic {@link QueryProcessor}
 * for performance reasons and we compute the result sets differently to make things faster and more memory efficient.
 * </p>
 * The simplified vertex processing only applies to loaded (i.e. non-mutated) vertices. The query can be configured
 * to only included loaded relations in the result set (which is needed, for instance, when computing index deltas in
 * {@link com.thinkaurelius.titan.graphdb.database.IndexSerializer}) via {@link #queryOnlyLoaded()}.
 * </p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexCentricQueryBuilder extends AbstractVertexCentricQueryBuilder<VertexCentricQueryBuilder> implements TitanVertexQuery<VertexCentricQueryBuilder> {

    private static final Logger log = LoggerFactory.getLogger(VertexCentricQueryBuilder.class);

    /**
    The base vertex of this query
     */
    private final InternalVertex vertex;

    /**
    Whether to query only for persisted edges, i.e. ignore any modifications to the vertex made in this transaction.
     This is achieved by using the {@link SimpleVertexQueryProcessor} for execution.
     */
    private boolean queryOnlyLoaded = false;

    public VertexCentricQueryBuilder(InternalVertex v) {
        super(v.tx());
        Preconditions.checkNotNull(v);
        Preconditions.checkArgument(!v.isRemoved(),"Cannot access a removed vertex: %s",v);
        this.vertex = v;
    }

    @Override
    protected VertexCentricQueryBuilder getThis() {
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    /**
     * Calling this method will cause this query to only included loaded (i.e. unmodified) relations in the
     * result set.
     * @return
     */
    public VertexCentricQueryBuilder queryOnlyLoaded() {
        queryOnlyLoaded=true;
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

//    @Override
//    protected EdgeSerializer.VertexConstraint getVertexConstraint() {
//        if (adjacentVertex != null && vertex.hasId() && adjacentVertex.hasId()) {
//            return new EdgeSerializer.VertexConstraint(vertex.getID(), adjacentVertex.getID());
//        } else return null;
//    }

    /**
     * Constructs a {@link VertexCentricQuery} for this query builder. The query construction and optimization
     * logic is taken from {@link AbstractVertexCentricQueryBuilder#constructQuery(com.thinkaurelius.titan.graphdb.internal.RelationCategory)}.
     * This method only adds the additional conditions that are based on the base vertex.
     *
     * @param returnType
     * @return
     */
    public VertexCentricQuery constructQuery(RelationCategory returnType) {
        BaseVertexCentricQuery vq = super.constructQuery(returnType);
        Condition<TitanRelation> condition = vq.getCondition();
        if (!vq.isEmpty()) {
            //Add adjacent-vertex and direction related conditions
            And<TitanRelation> newcond = (condition instanceof And) ? (And) condition : new And<TitanRelation>(condition);
            newcond.add(new DirectionCondition<TitanRelation>(vertex,getDirection()));
            if (getAdjacentVertex() != null)
                newcond.add(new IncidenceCondition<TitanRelation>(vertex,getAdjacentVertex()));
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
