package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.QueryProcessor;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.DirectionCondition;
import com.thinkaurelius.titan.graphdb.query.condition.IncidenceCondition;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of {@link TitanMultiVertexQuery} that extends {@link AbstractVertexCentricQueryBuilder}
 * for all the query building and optimization and adds only the execution logic in
 * {@link #relations(com.thinkaurelius.titan.graphdb.internal.RelationCategory)}.
 * </p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiVertexCentricQueryBuilder extends AbstractVertexCentricQueryBuilder<MultiVertexCentricQueryBuilder> implements TitanMultiVertexQuery<MultiVertexCentricQueryBuilder> {

    /**
     * The base vertices of this query
     */
    private final Set<InternalVertex> vertices;

    public MultiVertexCentricQueryBuilder(final StandardTitanTx tx) {
        super(tx);
        vertices = Sets.newHashSet();
    }

    @Override
    protected MultiVertexCentricQueryBuilder getThis() {
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Construction
	 * ---------------------------------------------------------------
	 */

    @Override
    public TitanMultiVertexQuery addVertex(TitanVertex vertex) {
        assert vertex != null;
        assert vertex instanceof InternalVertex;
        vertices.add((InternalVertex)vertex);
        return this;
    }

    @Override
    public TitanMultiVertexQuery addAllVertices(Collection<TitanVertex> vertices) {
        for (TitanVertex v : vertices) addVertex(v);
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    /**
     * Constructs the BaseVertexCentricQuery through {@link AbstractVertexCentricQueryBuilder#constructQuery(com.thinkaurelius.titan.graphdb.internal.RelationCategory)}.
     * If the query asks for an implicit key, the resulting map is computed and returned directly.
     * If the query is empty, a map that maps each vertex to an empty list is returned.
     * Otherwise, the query is executed for all vertices through the transaction which will effectively
     * pre-load the return result sets into the associated {@link com.thinkaurelius.titan.graphdb.vertices.CacheVertex} or
     * don't do anything at all if the vertex is new (and hence no edges in the storage backend).
     * After that, a map is constructed that maps each vertex to the corresponding VertexCentricQuery and wrapped
     * into a QueryProcessor. Hence, upon iteration the query will be executed like any other VertexCentricQuery
     * with the performance difference that the SliceQueries will have already been preloaded and not further
     * calls to the storage backend are needed.
     *
     * @param returnType
     * @return
     */
    protected Map<TitanVertex, Iterable<? extends TitanRelation>> relations(RelationCategory returnType) {
        Preconditions.checkArgument(!vertices.isEmpty(), "Need to add at least one vertex to query");
        Map<TitanVertex, Iterable<? extends TitanRelation>> result = new HashMap<TitanVertex, Iterable<? extends TitanRelation>>(vertices.size());
        if (isImplicitKeyQuery(returnType)) {
            for (InternalVertex v : vertices ) result.put(v,executeImplicitKeyQuery(v));
        } else {
            BaseVertexCentricQuery vq = super.constructQuery(returnType);
            if (!vq.isEmpty()) {
                for (BackendQueryHolder<SliceQuery> sq : vq.getQueries()) {
                    tx.executeMultiQuery(vertices, sq.getBackendQuery());
                }

                Condition<TitanRelation> condition = vq.getCondition();
                for (InternalVertex v : vertices) {
                    ///Add adjacent-vertex and direction related conditions (which depend on the base vertex) to the
                    // condition. Need to copy the condition in order to not overwrite a previous one.
                    And<TitanRelation> newcond = new And<TitanRelation>();
                    if (condition instanceof And) newcond.addAll((And) condition);
                    else newcond.add(condition);
                    newcond.add(new DirectionCondition<TitanRelation>(v, getDirection()));
                    if (getAdjacentVertex() != null)
                        newcond.add(new IncidenceCondition<TitanRelation>(v,getAdjacentVertex()));
                    VertexCentricQuery vqsingle = new VertexCentricQuery(v, newcond, vq.getDirection(), vq.getQueries(), vq.getOrders(), vq.getLimit());
                    result.put(v, new QueryProcessor<VertexCentricQuery, TitanRelation, SliceQuery>(vqsingle, tx.edgeProcessor));

                }
            } else {
                for (TitanVertex v : vertices)
                    result.put(v, Collections.EMPTY_LIST);
            }
        }
        return result;
    }


    @Override
    public Map<TitanVertex, Iterable<TitanEdge>> titanEdges() {
        return (Map) relations(RelationCategory.EDGE);
    }

    @Override
    public Map<TitanVertex, Iterable<TitanProperty>> properties() {
        return (Map) relations(RelationCategory.PROPERTY);
    }

    @Override
    public Map<TitanVertex, Iterable<TitanRelation>> relations() {
        return (Map) relations(RelationCategory.RELATION);
    }

    @Override
    public Map<TitanVertex, Iterable<TitanVertex>> vertices() {
        Map<TitanVertex, Iterable<TitanEdge>> base = titanEdges();
        Map<TitanVertex, Iterable<TitanVertex>> result = new HashMap<TitanVertex, Iterable<TitanVertex>>(base.size());
        for (Map.Entry<TitanVertex, Iterable<TitanEdge>> entry : base.entrySet()) {
            result.put(entry.getKey(), edges2Vertices(entry.getValue(), entry.getKey()));
        }
        return result;
    }

    @Override
    public Map<TitanVertex, VertexList> vertexIds() {
        Map<TitanVertex, Iterable<TitanEdge>> base = titanEdges();
        Map<TitanVertex, VertexList> result = new HashMap<TitanVertex, VertexList>(base.size());
        for (Map.Entry<TitanVertex, Iterable<TitanEdge>> entry : base.entrySet()) {
            result.put(entry.getKey(), edges2VertexIds(entry.getValue(), entry.getKey()));
        }
        return result;
    }

}
