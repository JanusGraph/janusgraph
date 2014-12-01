package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
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
import com.thinkaurelius.titan.graphdb.util.ElementHelper;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
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

    private static final Retriever<SliceQuery, EntryList> EXCEPTION_RETRIEVER = new Retriever<SliceQuery, EntryList>() {
        @Override
        public EntryList get(SliceQuery input) {
            throw new UnsupportedOperationException("Cannot retrieve edges or properties on this vertex");
        }
    };

    private PropertyMixing mixin = NO_MIXIN;
    private boolean swallowRetrievals = true;

    public PreloadedVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        assert lifecycle == ElementLifeCycle.Loaded : "Invalid lifecycle encountered: " + lifecycle;
    }

    public void setPropertyMixing(PropertyMixing mixin) {
        Preconditions.checkNotNull(mixin);
        Preconditions.checkArgument(this.mixin==NO_MIXIN,"A property mixing has already been set");
        this.mixin=mixin;
    }

    public void setExceptionOnRetrieve(boolean exceptionOnRetrieve) {
        swallowRetrievals = !exceptionOnRetrieve;
    }

    @Override
    public void addToQueryCache(final SliceQuery query, final EntryList entries) {
        super.addToQueryCache(query,entries);
    }

    public EntryList getFromCache(final SliceQuery query) {
        return queryCache.get(query);
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
        return super.loadRelations(query,swallowRetrievals?EMPTY_RETRIEVER:EXCEPTION_RETRIEVER);
    }

    @Override
    public<V> TitanVertexProperty<V> property(String key, V value) {
        return mixin.property(key,value);
    }

    @Override
    public <V> TitanVertexProperty<V> singleProperty(String key, V value, Object... keyValues) {
        TitanVertexProperty<V> p = mixin.singleProperty(key,value);
        ElementHelper.attachProperties(p,keyValues);
        return p;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... keys) {
        if (mixin==NO_MIXIN) return super.propertyIterator(keys);
        if (keys!=null && keys.length>0) {
            int count=0;
            for (int i = 0; i < keys.length; i++) if (mixin.supports(keys[i])) count++;
            if (count==0) return super.propertyIterator(keys);
            else if (count==keys.length) return mixin.propertyIterator(keys);
        }
        return (Iterator)com.google.common.collect.Iterators.concat(super.propertyIterator(keys),mixin.propertyIterator(keys));
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


    public interface PropertyMixing {

        public <V> Iterator<VertexProperty<V>> propertyIterator(String... keys);

        public boolean supports(String key);

        public <V>  TitanVertexProperty<V> property(String key, V value);

        public <V>  TitanVertexProperty<V> singleProperty(String key, V value);

    }

    private static PropertyMixing NO_MIXIN = new PropertyMixing() {
        @Override
        public <V> Iterator<VertexProperty<V>> propertyIterator(String... keys) {
            return Collections.emptyIterator();
        }

        @Override
        public boolean supports(String key) {
            return false;
        }

        @Override
        public <V> TitanVertexProperty<V> property(String key, V value) {
            return singleProperty(key,value);
        }

        @Override
        public <V> TitanVertexProperty<V> singleProperty(String key, V value) {
            throw new UnsupportedOperationException("Provided key is not supported: " + key);
        }
    };

}
