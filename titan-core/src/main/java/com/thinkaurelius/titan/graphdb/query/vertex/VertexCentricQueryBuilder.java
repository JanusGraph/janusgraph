package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.QueryProcessor;
import com.thinkaurelius.titan.graphdb.query.profile.QueryProfiler;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of {@link TitanVertexQuery} that extends {@link BasicVertexCentricQueryBuilder}
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
public class VertexCentricQueryBuilder extends BasicVertexCentricQueryBuilder<VertexCentricQueryBuilder> implements TitanVertexQuery<VertexCentricQueryBuilder> {

    private static final Logger log = LoggerFactory.getLogger(VertexCentricQueryBuilder.class);

    /**
    The base vertex of this query
     */
    private final InternalVertex vertex;

    public VertexCentricQueryBuilder(InternalVertex v) {
        super(v.tx());
        Preconditions.checkNotNull(v);
        this.vertex = v;
    }

    @Override
    protected VertexCentricQueryBuilder getThis() {
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
	 * ---------------------------------------------------------------
	 */

    protected<Q> Q execute(RelationCategory returnType, ResultConstructor<Q> resultConstructor) {
        BaseVertexCentricQuery bq = super.constructQuery(returnType);
        if (bq.isEmpty()) return resultConstructor.emptyResult();
        if (returnType==RelationCategory.PROPERTY && hasSingleType() && !hasQueryOnlyLoaded()
                && tx.getConfiguration().hasPropertyPrefetching()) {
            //Preload properties
            vertex.query().properties().iterator().hasNext();
        }

        if (isPartitionedVertex(vertex) && !hasQueryOnlyGivenVertex()) { //If it's a preloaded vertex we shouldn't preload data explicitly
            List<InternalVertex> vertices = allRequiredRepresentatives(vertex);
            profiler.setAnnotation(QueryProfiler.PARTITIONED_VERTEX_ANNOTATION,true);
            profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION,vertices.size());
            if (vertices.size()>1) {
                for (BackendQueryHolder<SliceQuery> sq : bq.getQueries()) {
                    tx.executeMultiQuery(vertices, sq.getBackendQuery(),sq.getProfiler());
                }
            }
        } else profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION,1);
        return resultConstructor.getResult(vertex,bq);
    }

    //#### RELATIONS

    @Override
    public Iterable<TitanEdge> edges() {
        return (Iterable)execute(RelationCategory.EDGE,new RelationConstructor());
    }

    @Override
    public Iterable<TitanVertexProperty> properties() {
        return (Iterable)(isImplicitKeyQuery(RelationCategory.PROPERTY)?
                executeImplicitKeyQuery(vertex):
                execute(RelationCategory.PROPERTY, new RelationConstructor()));
    }

    @Override
    public Iterable<TitanRelation> relations() {
        return (Iterable)(isImplicitKeyQuery(RelationCategory.RELATION)?
                executeImplicitKeyQuery(vertex):
                execute(RelationCategory.RELATION,new RelationConstructor()));
    }

    //#### VERTICES

    @Override
    public Iterable<TitanVertex> vertices() {
        return (Iterable)execute(RelationCategory.EDGE,new VertexConstructor());
    }

    @Override
    public VertexList vertexIds() {
        return execute(RelationCategory.EDGE,new VertexIdConstructor());
    }

}
