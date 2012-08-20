package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Set;

public abstract class LoadedEmptyTitanVertex implements InternalTitanVertex {

	private static final String errorName = "Empty Nodes";
	
	@Override
	public boolean addRelation(InternalRelation e, boolean isNew) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges");
	}
	
	@Override
	public void removeRelation(InternalRelation e) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges");
	}

	
	@Override
	public void loadedEdges(AtomicQuery query) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges");
	}

	@Override
	public boolean hasLoadedEdges(AtomicQuery query) {
		return true;
	}

	/* ---------------------------------------------------------------
	 * TitanRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public TitanQuery query() {
		throw new UnsupportedOperationException(errorName + " do not support querying");
	}

	@Override
	public Object getProperty(TitanKey key) {
		return null;
	}
	
	@Override
	public<O> O getProperty(TitanKey key, Class<O> clazz) {
		return null;
	}

	@Override
	public Object getProperty(String key) {
		return null;
	}

	@Override
	public <O> O getProperty(String key, Class<O> clazz) {
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
    public Iterable<InternalRelation> getRelations(AtomicQuery query, boolean loadRemaining) {
        return IterablesUtil.emptyIterable();
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
    public Object removeProperty(String key) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }


	@Override
	public TitanEdge addEdge(TitanLabel label, TitanVertex vertex) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges");
	}


	@Override
	public TitanEdge addEdge(String label, TitanVertex vertex) {
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
        if (hasID()) return Long.valueOf(getID());
        else return null;
    }

	@Override
	public boolean hasID() {
		return false;
	}
	
	@Override
	public void setID(long id) {
		throw new UnsupportedOperationException(errorName + " don't have an id");
	}

	@Override
	public boolean isAvailable() {
		return true;
	}
	
	@Override
	public boolean isAccessible() {
		return getTransaction().isOpen();
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
	public boolean isReferenceVertex() {
		return false;
	}
	
}
