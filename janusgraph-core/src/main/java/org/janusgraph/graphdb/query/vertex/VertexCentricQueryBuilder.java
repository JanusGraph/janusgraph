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
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.core.VertexList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.query.QueryProcessor;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import java.util.List;

/**
 * Implementation of {@link JanusGraphVertexQuery} that extends {@link BasicVertexCentricQueryBuilder}
 * for all the query building and optimization and adds only the execution logic in
 * {@link #constructQuery(org.janusgraph.graphdb.internal.RelationCategory)}. However, there is
 * one important special case: If the constructed query is simple 
 * then we use the {@link SimpleVertexQueryProcessor} to execute the query instead of the generic {@link QueryProcessor}
 * for performance reasons and we compute the result sets differently to make things faster and more memory efficient.
 * <p>
 * The simplified vertex processing only applies to loaded (i.e. non-mutated) vertices. The query can be configured
 * to only included loaded relations in the result set (which is needed, for instance, when computing index deltas in
 * {@link org.janusgraph.graphdb.database.IndexSerializer}) via {@link #queryOnlyLoaded()}.
 * <p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexCentricQueryBuilder extends BasicVertexCentricQueryBuilder<VertexCentricQueryBuilder> implements JanusGraphVertexQuery<VertexCentricQueryBuilder> {

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
    public Iterable<JanusGraphEdge> edges() {
        return (Iterable)execute(RelationCategory.EDGE,new RelationConstructor());
    }

    @Override
    public Iterable<JanusGraphVertexProperty> properties() {
        return (Iterable)(isImplicitKeyQuery(RelationCategory.PROPERTY)?
                executeImplicitKeyQuery(vertex):
                execute(RelationCategory.PROPERTY, new RelationConstructor()));
    }

    @Override
    public Iterable<JanusGraphRelation> relations() {
        return (Iterable)(isImplicitKeyQuery(RelationCategory.RELATION)?
                executeImplicitKeyQuery(vertex):
                execute(RelationCategory.RELATION,new RelationConstructor()));
    }

    //#### VERTICES

    @Override
    public Iterable<JanusGraphVertex> vertices() {
        return execute(RelationCategory.EDGE,new VertexConstructor());
    }

    @Override
    public VertexList vertexIds() {
        return execute(RelationCategory.EDGE,new VertexIdConstructor());
    }

}
