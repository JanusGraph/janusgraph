package com.thinkaurelius.titan.graphdb.vertices;


import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.QueryException;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsVertexUtil;
import com.thinkaurelius.titan.graphdb.query.AtomicTitanQuery;
import com.thinkaurelius.titan.graphdb.query.ComplexTitanQuery;
import com.thinkaurelius.titan.graphdb.query.InternalTitanQuery;
import com.thinkaurelius.titan.graphdb.entitystatus.InMemoryElement;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.Set;

public abstract class AbstractTitanVertex implements InternalTitanVertex {

	protected final InternalTitanTransaction tx;


	public AbstractTitanVertex(InternalTitanTransaction g) {
		assert g!=null;
		tx = g;
	}
	
	@Override
	public InternalTitanTransaction getTransaction() {
		return tx;
	}

    /* ---------------------------------------------------------------
      * In memory handling
      * ---------------------------------------------------------------
      */
	
	@Override
	public int hashCode() {
		if (hasID()) {
			return VertexUtil.getIDHashCode(this);
		} else {
			assert isNew();
			return super.hashCode();
		}
		
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof InternalTitanVertex)) return false;
		InternalTitanVertex other = (InternalTitanVertex)oth;
		return VertexUtil.equalIDs(this, other);
	}
	
	@Override
	public InternalTitanVertex clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}
    
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

	
	/* ---------------------------------------------------------------
	 * Changing Edges
	 * ---------------------------------------------------------------
	 */

	
	@Override
	public void loadedEdges(InternalTitanQuery query) {
		throw new UnsupportedOperationException("Relation loading is not supported on in memory vertices");
	}

	@Override
	public boolean hasLoadedEdges(InternalTitanQuery query) {
		return isNew();
	}

	protected synchronized void ensureLoadedEdges(InternalTitanQuery query) {
		if (!hasLoadedEdges(query)) tx.loadRelations(query);
	}

	@Override
	public void remove() {
		VertexUtil.checkAvailability(this);
        VertexUtil.prepareForRemoval(this);
        tx.deleteVertex(this);
	}

	/* ---------------------------------------------------------------
	 * TitanRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public TitanQuery query() {
		return tx.query(this);
	}

    @Override
    public Set<String> getPropertyKeys() {
        return BlueprintsVertexUtil.getPropertyKeys(this);
    }

	@Override
	public Object getProperty(TitanKey key) {
		try {
			TitanProperty p = Iterators.getOnlyElement(new AtomicTitanQuery(this).type(key).propertyIterator(), null);
			if (p==null) return null;
			else return p.getAttribute();
		} catch (IllegalArgumentException e) {
			throw new QueryException("Multiple properties of specified type: " + key,e);
		}
	}
	
	@Override
	public Object getProperty(String key) {
        if (!tx.containsType(key)) return null;
		else return getProperty(tx.getPropertyKey(key));
	}

	@Override
	public<O> O getProperty(TitanKey key, Class<O> clazz) {
		try {
			TitanProperty p = Iterators.getOnlyElement(new AtomicTitanQuery(this).type(key).propertyIterator(), null);
			if (p==null) return null;
			else return p.getAttribute(clazz);
		} catch (IllegalArgumentException e) {
			throw new QueryException("Multiple properties of specified type: " + key,e);
		}
	}
	
	@Override
	public<O> O getProperty(String key, Class<O> clazz) {
        if (!tx.containsType(key)) return null;
		else return getProperty(tx.getPropertyKey(key), clazz);
	}

	@Override
	public Iterable<TitanProperty> getProperties() {
		return new AtomicTitanQuery(this).properties();
	}

	@Override
	public Iterable<TitanProperty> getProperties(TitanKey key) {
		return new AtomicTitanQuery(this).type(key).properties();
	}

	@Override
	public Iterable<TitanProperty> getProperties(String key) {
        return new AtomicTitanQuery(this).keys(key).properties();
	}

    
    
	@Override
	public Iterable<TitanEdge> getEdges() {
		return new AtomicTitanQuery(this).titanEdges();
	}


	@Override
	public Iterable<TitanEdge> getTitanEdges(Direction dir, TitanLabel... labels) {
		return new ComplexTitanQuery(this).direction(dir).types(labels).titanEdges();
	}

	@Override
	public Iterable<Edge> getEdges(Direction dir, String... labels) {
        return new ComplexTitanQuery(this).direction(dir).labels(labels).edges();
	}
	
	@Override
	public Iterable<TitanRelation> getRelations() {
		return new AtomicTitanQuery(this).relations();
	}

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return new ComplexTitanQuery(this).direction(direction).labels(labels).vertices();
    }

	
	/* ---------------------------------------------------------------
	 * TitanRelation Counts
	 * ---------------------------------------------------------------
	 */


	@Override
	public long getPropertyCount() {
		return new AtomicTitanQuery(this).propertyCount();
	}


	@Override
	public long getEdgeCount() {
		return new AtomicTitanQuery(this).count();
	}
	
	@Override
	public boolean isConnected() {
		return !Iterables.isEmpty(getEdges());
	}

	
	/* ---------------------------------------------------------------
	 * Convenience Methods for TitanElement Creation
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public TitanProperty addProperty(TitanKey key, Object attribute) {
		return tx.addProperty(this, key, attribute);
	}


	@Override
	public TitanProperty addProperty(String key, Object attribute) {
		return tx.addProperty(this, key, attribute);
	}

    @Override
    public void setProperty(String key, Object value) {
        BlueprintsVertexUtil.setProperty(this,tx,key,value);
    }

    @Override
    public Object removeProperty(String key) {
        return BlueprintsVertexUtil.removeProperty(this,tx,key);
    }


	@Override
	public TitanEdge addEdge(TitanLabel label, TitanVertex vertex) {
		return tx.addEdge(this, vertex, label);
	}


	@Override
	public TitanEdge addEdge(String label, TitanVertex vertex) {
		return tx.addEdge(this, vertex, label);
	}

	
	/* ---------------------------------------------------------------
	 * In Memory TitanElement
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		return InMemoryElement.instance.getID();
	}

    @Override
    public Object getId() {
        if (hasID()) return Long.valueOf(getID());
        else return null;
    }

	@Override
	public boolean hasID() {
		return InMemoryElement.instance.hasID();
	}
	
	@Override
	public void setID(long id) {
		InMemoryElement.instance.setID(id);
	}


	@Override
	public boolean isAccessible() {
		return tx.isOpen();
	}

	@Override
	public boolean isAvailable() {
		return InMemoryElement.instance.isAvailable();
	}

	@Override
	public boolean isRemoved() {
		return InMemoryElement.instance.isRemoved();
	}

	@Override
	public boolean isLoaded() {
		return InMemoryElement.instance.isLoaded();
	}

	@Override
	public boolean isModified() {
		return InMemoryElement.instance.isModified();
	}

	@Override
	public boolean isNew() {
		return InMemoryElement.instance.isNew();
	}

	@Override
	public boolean isReferenceVertex() {
		return InMemoryElement.instance.isReferenceVertex();
	}



	
	
}
