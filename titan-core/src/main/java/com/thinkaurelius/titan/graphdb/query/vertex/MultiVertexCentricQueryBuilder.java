package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

import java.util.*;

/**
 * Implementation of {@link TitanMultiVertexQuery} that extends {@link BasicVertexCentricQueryBuilder}
 * for all the query building and optimization and adds only the execution logic in
 * {@link #execute(com.thinkaurelius.titan.graphdb.internal.RelationCategory, BasicVertexCentricQueryBuilder.ResultConstructor)}.
 * </p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiVertexCentricQueryBuilder extends BasicVertexCentricQueryBuilder<MultiVertexCentricQueryBuilder> implements TitanMultiVertexQuery<MultiVertexCentricQueryBuilder> {

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
        vertices.add(((InternalVertex)vertex).it());
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
     * Constructs the BaseVertexCentricQuery through {@link BasicVertexCentricQueryBuilder#constructQuery(com.thinkaurelius.titan.graphdb.internal.RelationCategory)}.
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
    protected<Q> Map<TitanVertex,Q> execute(RelationCategory returnType, ResultConstructor<Q> resultConstructor) {
        Preconditions.checkArgument(!vertices.isEmpty(), "Need to add at least one vertex to query");
        Map<TitanVertex, Q> result = new HashMap<TitanVertex, Q>(vertices.size());
        BaseVertexCentricQuery bq = super.constructQuery(returnType);
        if (!bq.isEmpty()) {
            for (BackendQueryHolder<SliceQuery> sq : bq.getQueries()) {
                Set<InternalVertex> adjVertices = Sets.newHashSet(vertices);
                for (InternalVertex v : vertices) {
                    if (isPartitionedVertex(v)) {
                        adjVertices.remove(v);
                        adjVertices.addAll(allRepresentatives(v));
                    }
                }
                tx.executeMultiQuery(adjVertices, sq.getBackendQuery());
            }
            for (InternalVertex v : vertices) {
                result.put(v, resultConstructor.getResult(v, bq));
            }
        } else {
            for (TitanVertex v : vertices)
                result.put(v, resultConstructor.emptyResult());
        }
        return result;
    }

    public Map<TitanVertex, Iterable<? extends TitanRelation>> executeImplicitKeyQuery() {
        return new HashMap<TitanVertex, Iterable<? extends TitanRelation>>(vertices.size()){{
            for (InternalVertex v : vertices ) put(v,executeImplicitKeyQuery(v));
        }};
    }

    @Override
    public Map<TitanVertex, Iterable<TitanEdge>> titanEdges() {
        return (Map) execute(RelationCategory.EDGE, new RelationConstructor());
    }

    @Override
    public Map<TitanVertex, Iterable<TitanProperty>> properties() {
        return (Map)(isImplicitKeyQuery(RelationCategory.PROPERTY)?
                executeImplicitKeyQuery():
                execute(RelationCategory.PROPERTY, new RelationConstructor()));
    }

    @Override
    public Map<TitanVertex, Iterable<TitanRelation>> relations() {
        return (Map)(isImplicitKeyQuery(RelationCategory.RELATION)?
                executeImplicitKeyQuery():
                execute(RelationCategory.RELATION, new RelationConstructor()));
    }

    @Override
    public Map<TitanVertex, Iterable<TitanVertex>> vertices() {
        return execute(RelationCategory.EDGE, new VertexConstructor());
    }

    @Override
    public Map<TitanVertex, VertexList> vertexIds() {
        return execute(RelationCategory.EDGE, new VertexIdConstructor());
    }

    @Override
    public QueryDescription describeForEdges() {
        return describe(vertices.size(),RelationCategory.EDGE);
    }

    @Override
    public QueryDescription describeForProperties() {
        return describe(vertices.size(),RelationCategory.PROPERTY);
    }

}
