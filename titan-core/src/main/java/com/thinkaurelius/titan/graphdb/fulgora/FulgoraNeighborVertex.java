package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemRelationType;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraNeighborVertex implements InternalVertex, Vertex.Iterators {

    private final long id;
    private final FulgoraExecutor executor;

    public FulgoraNeighborVertex(long id, FulgoraExecutor executor) {
        this.id = id;
        this.executor = executor;
    }

    private static UnsupportedOperationException getAccessException() {
        return new UnsupportedOperationException();
    }


    @Override
    public<A> A value(String key) {
        if (key.equals(executor.stateKey)) {
            return (A)executor.getVertexState(longId());
        }
        SystemRelationType t = SystemTypeManager.getSystemType(key);
        if (t!=null && t instanceof ImplicitKey) return ((ImplicitKey)t).computeProperty(this);
        throw getAccessException();
    }

    @Override
    public <O> O value(PropertyKey key) {
        if (key instanceof ImplicitKey) return ((ImplicitKey)key).computeProperty(this);
        throw getAccessException();
    }

    @Override
    public String label() {
        return vertexLabel().name();
    }

    @Override
    public VertexLabel vertexLabel() {
        throw getAccessException();
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public Object id() {
        return longId();
    }

    @Override
    public long longId() {
        return id;
    }

    @Override
    public boolean hasId() {
        return true;
    }

    @Override
    public void remove() {
        throw getAccessException();
    }

    @Override
    public InternalVertex it() {
        return this;
    }

    @Override
    public StandardTitanTx tx() {
        return executor.tx();
    }

    @Override
    public void setId(long id) {
        throw getAccessException();
    }

    @Override
    public byte getLifeCycle() {
        return ElementLifeCycle.Loaded;
    }

    @Override
    public boolean isInvisible() {
        return false;
    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw getAccessException();
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw getAccessException();
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        throw getAccessException();
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        throw getAccessException();
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        throw getAccessException();
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
    public<V> TitanVertexProperty<V> property(String key, V attribute) {
        throw getAccessException();
    }

    @Override
    public VertexCentricQueryBuilder query() {
        throw getAccessException();
    }

    @Override
    public TitanEdge addEdge(String s, Vertex vertex, Object... keyValues) {
        throw getAccessException();
    }

    @Override
    public boolean isModified() {
        return false;
    }

    /* ---------------------------------------------------------------
	 * TinkPop Iterators Method
	 * ---------------------------------------------------------------
	 */

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Edge> edgeIterator(Direction direction, String... strings) {
        throw getAccessException();
    }

    @Override
    public Iterator<Vertex> vertexIterator(Direction direction, String... strings) {
        throw getAccessException();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... strings) {
        throw getAccessException();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(String... strings) {
        throw getAccessException();
    }
}
