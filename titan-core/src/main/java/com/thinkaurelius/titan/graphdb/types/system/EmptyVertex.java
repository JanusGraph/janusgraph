package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Collection;
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
    public Collection<Entry> loadRelations(SliceQuery query, Retriever<SliceQuery, List<Entry>> lookup) {
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
    public <O> O getProperty(TitanKey key) {
        return null;
    }

    @Override
    public <O> O getProperty(String key) {
        return null;
    }


    @Override
    public Iterable<TitanProperty> getProperties() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanProperty> getProperties(TitanKey key) {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanProperty> getProperties(String key) {
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
    public Iterable<TitanEdge> getTitanEdges(Direction dir, TitanLabel... labels) {
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
    public TitanProperty addProperty(TitanKey key, Object attribute) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }


    @Override
    public TitanProperty addProperty(String key, Object attribute) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public void setProperty(TitanKey key, Object value) {
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
    public <O> O removeProperty(TitanType type) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }


    @Override
    public TitanEdge addEdge(TitanLabel label, TitanVertex vertex) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }


    @Override
    public TitanEdge addEdge(String label, TitanVertex vertex) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public Edge addEdge(String s, Vertex vertex) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

	/* ---------------------------------------------------------------
	 * In Memory TitanElement
	 * ---------------------------------------------------------------
	 */

    @Override
    public long getID() {
        throw new UnsupportedOperationException(errorName + " don't have an ID");
    }

    @Override
    public Object getId() {
        return hasId() ? getID() : null;
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
    public void setID(long id) {
        throw new UnsupportedOperationException(errorName + " don't have an id");
    }

    @Override
    public byte getLifeCycle() {
        return ElementLifeCycle.Loaded;
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

    @Override
    public int compareTo(TitanElement titanElement) {
        return Longs.compare(getID(),titanElement.getID());
    }
}