package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraVertex<S> extends CacheVertex {

    private final FulgoraExecutor<S> executor;
    private Map<String,Object> processedProperties;

    FulgoraVertex(StandardTitanTx tx, long id,
                  final FulgoraExecutor<S> executor) {
        super(tx, id, ElementLifeCycle.Loaded);
        this.executor = executor;
    }

    void setProcessedProperties(Map<String,Object> processedProperties) {
        assert processedProperties!=null;
        this.processedProperties=processedProperties;
    }

    @Override
    public<A> A getProperty(String key) {
        if (key.equals(executor.stateKey)) {
            return (A)executor.getVertexState(getID());
        } else if (processedProperties.containsKey(key)) {
            return (A)processedProperties.get(key);
        } else return null;
    }

    @Override
    public<A> A getProperty(PropertyKey key) {
        return getProperty(key.getName());
    }

    @Override
    protected void addToQueryCache(final SliceQuery query, final EntryList entries) {
        super.addToQueryCache(query,entries);
    }

    @Override
    public VertexCentricQueryBuilder query() {
        throw new UnsupportedOperationException("All edges and properties have been pre-processed. Retrieve them via getProperty");
        //return new QueryBuilder();
    }

    /**
     * Special handling to provide access to the label even though its a different vertex by accessing it directly
     * in the enclosing transaction which will cache all vertex labels
     * @return
     */
    @Override
    public VertexLabel getVertexLabel() {
        Long labelid = getProperty(BaseLabel.VertexLabelEdge.getName());
        if (labelid==null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)tx().getInternalVertex(labelid);
    }

    /*
    ############ DEFAULT METHODS OVERWRITE ###############
     */

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return true;
    }

    @Override
    public boolean hasRemovedRelations() {
        return false;
    }

    @Override
    public boolean hasAddedRelations() {
        return false;
    }

    @Override
    public byte getLifeCycle() {
        return ElementLifeCycle.Loaded;
    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw new UnsupportedOperationException("Element mutation is not supported in OLAP");
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw new UnsupportedOperationException("Element mutation is not supported in OLAP");
    }

//    private class QueryBuilder extends VertexCentricQueryBuilder {
//
//        public QueryBuilder() {
//            super(FulgoraVertex.this);
//        }
//
//         /* ---------------------------------------------------------------
//         * Query Execution
//         * ---------------------------------------------------------------
//         */
//
//        private Iterable<TitanRelation> relations(RelationCategory returnType) {
//            return new QueryProcessor<VertexCentricQuery,TitanRelation,SliceQuery>(constructQuery(FulgoraVertex.this,returnType), executor.edgeProcessor);
//        }
//
//        @Override
//        public Iterable<TitanEdge> titanEdges() {
//            return (Iterable) relations(RelationCategory.EDGE);
//        }
//
//        @Override
//        public Iterable<TitanProperty> properties() {
//            return (Iterable) relations(RelationCategory.PROPERTY);
//        }
//
//        @Override
//        public Iterable<TitanRelation> relations() {
//            return relations(RelationCategory.RELATION);
//        }
//
//        @Override
//        public Iterable<Edge> edges() {
//            return (Iterable) titanEdges();
//        }
//
//        //#### VERTICES
//
//        @Override
//        public Iterable<Vertex> vertices() {
//            return (Iterable) edges2Vertices((Iterable)relations(RelationCategory.EDGE), FulgoraVertex.this);
//        }
//
//        @Override
//        public VertexList vertexIds() {
//            return edges2VertexIds((Iterable)relations(RelationCategory.EDGE), FulgoraVertex.this);
//        }
//
//        //#### COUNTS
//
//        @Override
//        public long count() {
//            return Iterables.size(titanEdges());
//        }
//
//        @Override
//        public long propertyCount() {
//            return Iterables.size(properties());
//        }
//
//
//    }

}
