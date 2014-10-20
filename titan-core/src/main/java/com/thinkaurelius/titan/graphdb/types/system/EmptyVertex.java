package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EmptyVertex implements InternalVertex {

    private static final String errorName = "Empty vertex";

	/* ---------------------------------------------------------------
     * TitanRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */

    @Override
    public VertexCentricQueryBuilder query() {
        throw new UnsupportedOperationException(errorName + " do not support querying");
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return false;
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
    public String getLabel() {
        return getVertexLabel().name();
    }

    @Override
    public VertexLabel getVertexLabel() {
        return BaseVertexLabel.DEFAULT_VERTEXLABEL;
    }

    @Override
    public <O> O getProperty(PropertyKey key) {
        if (key instanceof ImplicitKey) return ((ImplicitKey)key).computeProperty(this);
        return null;
    }

    @Override
    public <O> O getProperty(String key) {
        if (!tx().containsRelationType(key)) return null;
        else return getProperty(tx().getPropertyKey(key));
    }


    @Override
    public Iterable<TitanVertexProperty> getProperties() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanVertexProperty> getProperties(PropertyKey key) {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanVertexProperty> getProperties(String key) {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return ImmutableSet.of();
    }

    @Override
    public Iterable<TitanEdge> getEdges() {
        return IterablesUtil.emptyIterable();
    }


    @Override
    public Iterable<TitanEdge> getTitanEdges(Direction dir, EdgeLabel... labels) {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<Edge> getEdges(Direction dir, String... labels) {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanRelation> getRelations() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return IterablesUtil.emptyIterable();
    }

	/* ---------------------------------------------------------------
	 * TitanRelation Counts
	 * ---------------------------------------------------------------
	 */


    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Override
    public long getEdgeCount() {
        return 0;
    }

    @Override
    public boolean isConnected() {
        return false;
    }



	/* ---------------------------------------------------------------
	 * Convenience Methods for TitanElement Creation
	 * ---------------------------------------------------------------
	 */

    @Override
    public TitanVertexProperty addProperty(PropertyKey key, Object attribute) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }


    @Override
    public TitanVertexProperty addProperty(String key, Object attribute) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public void setProperty(PropertyKey key, Object value) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public <O> O removeProperty(String key) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public <O> O removeProperty(RelationType type) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }


    @Override
    public TitanEdge addEdge(EdgeLabel label, TitanVertex vertex) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }


    @Override
    public TitanEdge addEdge(String label, TitanVertex vertex) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public Edge addEdge(String s, Vertex vertex, Object... keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public Vertex.Iterators iterators() {
        return Iterators.INSTANCE;
    }

    public static class Iterators implements Vertex.Iterators {

        public static final Iterators INSTANCE = new Iterators();

        private Iterators() {}

        @Override
        public Iterator<Edge> edgeIterator(Direction direction, int i, String... strings) {
            return com.google.common.collect.Iterators.emptyIterator();
        }

        @Override
        public Iterator<Vertex> vertexIterator(Direction direction, int i, String... strings) {
            return com.google.common.collect.Iterators.emptyIterator();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> propertyIterator(String... strings) {
            return com.google.common.collect.Iterators.emptyIterator();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(String... strings) {
            return com.google.common.collect.Iterators.emptyIterator();
        }

    }

	/* ---------------------------------------------------------------
	 * In Memory TitanElement
	 * ---------------------------------------------------------------
	 */

    @Override
    public long getLongId() {
        throw new UnsupportedOperationException(errorName + " don't have an ID");
    }

    @Override
    public Object getId() {
        return hasId() ? getLongId() : null;
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(errorName + " cannot be removed");
    }

    @Override
    public void setId(long id) {
        throw new UnsupportedOperationException(errorName + " don't have an id");
    }

    @Override
    public byte getLifeCycle() {
        return ElementLifeCycle.Loaded;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public InternalVertex it() {
        return this;
    }

    @Override
    public StandardTitanTx tx() {
        throw new UnsupportedOperationException(errorName + " don't have an associated transaction");
    }

}