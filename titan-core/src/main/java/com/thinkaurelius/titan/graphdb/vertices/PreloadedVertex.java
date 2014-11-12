package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PreloadedVertex extends CacheVertex {

    private static final Retriever<SliceQuery, EntryList> EMPTY_RETRIEVER = new Retriever<SliceQuery, EntryList>() {
        @Override
        public EntryList get(SliceQuery input) {
            return EntryList.EMPTY_LIST;
        }
    };

    public PreloadedVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        assert lifecycle == ElementLifeCycle.Loaded : "Invalid lifecycle encountered: " + lifecycle;
    }

    @Override
    public void addToQueryCache(final SliceQuery query, final EntryList entries) {
        super.addToQueryCache(query,entries);
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        if (super.getQueryCacheSize()>0) return super.query();
        else throw stubVertexException();
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
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        return super.loadRelations(query,EMPTY_RETRIEVER);
    }

    @Override
    public<V> TitanVertexProperty<V> property(String key, V attribute) {
        throw stubVertexException();
    }

    @Override
    public TitanEdge addEdge(String s, Vertex vertex, Object... keyValues) {
        throw stubVertexException();
    }

    @Override
    public void remove() {
        throw stubVertexException();
    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw stubVertexException();
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw stubVertexException();
    }

    private static UnsupportedOperationException stubVertexException() {
        return new UnsupportedOperationException("Operation not supported on a stub vertex");
    }


}
