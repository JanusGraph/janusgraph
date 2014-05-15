package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.olap.StateInitializer;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.QueryExecutor;
import com.thinkaurelius.titan.graphdb.query.QueryProcessor;
import com.thinkaurelius.titan.graphdb.query.vertex.SimpleVertexQueryProcessor;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQuery;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.graphdb.vertices.AbstractVertex;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraVertex<S> extends CacheVertex {

    private final FulgoraExecutor<S> executor;

    FulgoraVertex(StandardTitanTx tx, long id,
                  final FulgoraExecutor<S> executor) {
        super(tx, id, ElementLifeCycle.Loaded);
        this.executor = executor;
    }

    @Override
    public<A> A getProperty(String key) {
        if (key.equals(executor.stateKey)) {
            return (A)executor.getVertexState(getID());
        } else return super.getProperty(key);
    }

    @Override
    public void setProperty(String key, Object value) {
        if (key.equals(executor.stateKey)) {
            executor.setVertexState(getID(),(S)value);
        } else super.setProperty(key,value);
    }

    @Override
    protected void addToQueryCache(final SliceQuery query, final EntryList entries) {
        super.addToQueryCache(query,entries);
    }

    @Override
    public VertexCentricQueryBuilder query() {
        return new QueryBuilder();
    }

    /**
     * Special handling to provide access to the label even though its a different vertex by accessing it directly
     * in the enclosing transaction which will cache all vertex labels
     * @return
     */
    @Override
    public VertexLabel getVertexLabel() {
        FulgoraNeighborVertex label = (FulgoraNeighborVertex)Iterables.getOnlyElement(query().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).vertices(),null);
        if (label==null) return SystemTypeManager.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)tx().getExistingVertex(label.getID());
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

    private class QueryBuilder extends VertexCentricQueryBuilder {

        public QueryBuilder() {
            super(FulgoraVertex.this);
        }

         /* ---------------------------------------------------------------
         * Query Execution
         * ---------------------------------------------------------------
         */

        private Iterable<TitanRelation> relations(RelationCategory returnType) {
            return new QueryProcessor<VertexCentricQuery,TitanRelation,SliceQuery>(constructQuery(returnType), executor.edgeProcessor);
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
            return (Iterable) edges2Vertices((Iterable)relations(RelationCategory.EDGE), FulgoraVertex.this);
        }

        @Override
        public VertexList vertexIds() {
            return edges2VertexIds((Iterable)relations(RelationCategory.EDGE), FulgoraVertex.this);
        }

        //#### COUNTS

        @Override
        public long count() {
            return Iterables.size(titanEdges());
        }

        @Override
        public long propertyCount() {
            return Iterables.size(properties());
        }


    }

}
