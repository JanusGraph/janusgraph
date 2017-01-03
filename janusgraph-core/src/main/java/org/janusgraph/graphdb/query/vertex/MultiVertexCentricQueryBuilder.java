package org.janusgraph.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.janusgraph.core.*;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Implementation of {@link JanusMultiVertexQuery} that extends {@link BasicVertexCentricQueryBuilder}
 * for all the query building and optimization and adds only the execution logic in
 * {@link #execute(org.janusgraph.graphdb.internal.RelationCategory, BasicVertexCentricQueryBuilder.ResultConstructor)}.
 * </p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiVertexCentricQueryBuilder extends BasicVertexCentricQueryBuilder<MultiVertexCentricQueryBuilder> implements JanusMultiVertexQuery<MultiVertexCentricQueryBuilder> {

    /**
     * The base vertices of this query
     */
    private final Set<InternalVertex> vertices;

    public MultiVertexCentricQueryBuilder(final StandardJanusTx tx) {
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
    public JanusMultiVertexQuery addVertex(Vertex vertex) {
        assert vertex != null;
        assert vertex instanceof InternalVertex;
        vertices.add(((InternalVertex)vertex).it());
        return this;
    }

    @Override
    public JanusMultiVertexQuery addAllVertices(Collection<? extends Vertex> vertices) {
        for (Vertex v : vertices) addVertex(v);
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    /**
     * Constructs the BaseVertexCentricQuery through {@link BasicVertexCentricQueryBuilder#constructQuery(org.janusgraph.graphdb.internal.RelationCategory)}.
     * If the query asks for an implicit key, the resulting map is computed and returned directly.
     * If the query is empty, a map that maps each vertex to an empty list is returned.
     * Otherwise, the query is executed for all vertices through the transaction which will effectively
     * pre-load the return result sets into the associated {@link org.janusgraph.graphdb.vertices.CacheVertex} or
     * don't do anything at all if the vertex is new (and hence no edges in the storage backend).
     * After that, a map is constructed that maps each vertex to the corresponding VertexCentricQuery and wrapped
     * into a QueryProcessor. Hence, upon iteration the query will be executed like any other VertexCentricQuery
     * with the performance difference that the SliceQueries will have already been preloaded and not further
     * calls to the storage backend are needed.
     *
     * @param returnType
     * @return
     */
    protected<Q> Map<JanusVertex,Q> execute(RelationCategory returnType, ResultConstructor<Q> resultConstructor) {
        Preconditions.checkArgument(!vertices.isEmpty(), "Need to add at least one vertex to query");
        Map<JanusVertex, Q> result = new HashMap<JanusVertex, Q>(vertices.size());
        BaseVertexCentricQuery bq = super.constructQuery(returnType);
        profiler.setAnnotation(QueryProfiler.MULTIQUERY_ANNOTATION,true);
        profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION,vertices.size());
        if (!bq.isEmpty()) {
            for (BackendQueryHolder<SliceQuery> sq : bq.getQueries()) {
                Set<InternalVertex> adjVertices = Sets.newHashSet(vertices);
                for (InternalVertex v : vertices) {
                    if (isPartitionedVertex(v)) {
                        profiler.setAnnotation(QueryProfiler.PARTITIONED_VERTEX_ANNOTATION,true);
                        adjVertices.remove(v);
                        adjVertices.addAll(allRequiredRepresentatives(v));
                    }
                }
                //Overwrite with more accurate size accounting for partitioned vertices
                profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION,adjVertices.size());
                tx.executeMultiQuery(adjVertices, sq.getBackendQuery(), sq.getProfiler());
            }
            for (InternalVertex v : vertices) {
                result.put(v, resultConstructor.getResult(v, bq));
            }
        } else {
            for (JanusVertex v : vertices)
                result.put(v, resultConstructor.emptyResult());
        }
        return result;
    }

    public Map<JanusVertex, Iterable<? extends JanusRelation>> executeImplicitKeyQuery() {
        return new HashMap<JanusVertex, Iterable<? extends JanusRelation>>(vertices.size()){{
            for (InternalVertex v : vertices ) put(v,executeImplicitKeyQuery(v));
        }};
    }

    @Override
    public Map<JanusVertex, Iterable<JanusEdge>> edges() {
        return (Map) execute(RelationCategory.EDGE, new RelationConstructor());
    }

    @Override
    public Map<JanusVertex, Iterable<JanusVertexProperty>> properties() {
        return (Map)(isImplicitKeyQuery(RelationCategory.PROPERTY)?
                executeImplicitKeyQuery():
                execute(RelationCategory.PROPERTY, new RelationConstructor()));
    }

    @Override
    public Map<JanusVertex, Iterable<JanusRelation>> relations() {
        return (Map)(isImplicitKeyQuery(RelationCategory.RELATION)?
                executeImplicitKeyQuery():
                execute(RelationCategory.RELATION, new RelationConstructor()));
    }

    @Override
    public Map<JanusVertex, Iterable<JanusVertex>> vertices() {
        return execute(RelationCategory.EDGE, new VertexConstructor());
    }

    @Override
    public Map<JanusVertex, VertexList> vertexIds() {
        return execute(RelationCategory.EDGE, new VertexIdConstructor());
    }

}
