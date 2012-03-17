package com.thinkaurelius.titan.graphdb.vertices;


import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.exceptions.QueryException;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edgequery.StandardEdgeQuery;
import com.thinkaurelius.titan.graphdb.entitystatus.InMemoryEntity;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

import java.util.Iterator;

public abstract class AbstractNode implements InternalNode {

	protected final GraphTx tx;


	public AbstractNode(GraphTx g) {
		assert g!=null;
		tx = g;
	}
	
	@Override
	public GraphTx getTransaction() {
		return tx;
	}

    /* ---------------------------------------------------------------
      * In memory handling
      * ---------------------------------------------------------------
      */
	
	@Override
	public int hashCode() {
		if (hasID()) {
			return NodeUtil.getIDHashCode(this);
		} else {
			assert isNew();
			return super.hashCode();
		}
		
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof InternalNode)) return false;
		InternalNode other = (InternalNode)oth;
		return NodeUtil.equalIDs(this, other);
	}
	
	@Override
	public InternalNode clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}

	
	/* ---------------------------------------------------------------
	 * Changing Edges
	 * ---------------------------------------------------------------
	 */

	
	@Override
	public void loadedEdges(InternalEdgeQuery query) {
		throw new UnsupportedOperationException("Edge loading is not supported on in memory vertices!");
	}

	@Override
	public boolean hasLoadedEdges(InternalEdgeQuery query) {
		return isNew();
	}

	protected synchronized void ensureLoadedEdges(InternalEdgeQuery query) {
		if (!hasLoadedEdges(query)) tx.loadEdges(query);
	}

	@Override
	public void delete() {
		NodeUtil.checkAvailability(this);
		if (Iterables.size(getEdges(StandardEdgeQuery.queryAll(this),true))>0)
			throw new IllegalStateException("Cannot delete node since it is still connected!");
        tx.deleteNode(this);
	}

	/* ---------------------------------------------------------------
	 * Edge Iteration/Access
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public EdgeQuery edgeQuery() {
		return tx.makeEdgeQuery(this);
	}

	
	@Override
	public Object getAttribute(PropertyType type) {
		try {
			Property p = Iterators.getOnlyElement(new StandardEdgeQuery(this).withEdgeType(type).getPropertyIterator(), null);
			if (p==null) return null;
			else return p.getAttribute();
		} catch (IllegalArgumentException e) {
			throw new QueryException("Multiple properties of specified type: " + type,e);
		}
	}
	
	@Override
	public Object getAttribute(String type) {
        if (!tx.containsEdgeType(type)) return null;
		else return getAttribute(tx.getPropertyType(type));
	}

	@Override
	public<O> O getAttribute(PropertyType type, Class<O> clazz) {
		try {
			Property p = Iterators.getOnlyElement(new StandardEdgeQuery(this).withEdgeType(type).getPropertyIterator(), null);
			if (p==null) return null;
			else return p.getAttribute(clazz);
		} catch (IllegalArgumentException e) {
			throw new QueryException("Multiple properties of specified type: " + type,e);
		}
	}
	
	@Override
	public<O> O getAttribute(String type, Class<O> clazz) {
        if (!tx.containsEdgeType(type)) return null;
		else return getAttribute(tx.getPropertyType(type),clazz);
	}

	@Override
	public String getString(PropertyType type) {
		return getAttribute(type, String.class);
	}
	
	@Override
	public String getString(String type) {
		return getAttribute(type, String.class);
	}
	
	@Override
	public Number getNumber(PropertyType type) {
		return getAttribute(type, Number.class);
	}
	
	@Override
	public Number getNumber(String type) {
		return getAttribute(type, Number.class);
	}
	
	@Override
	public Iterable<Property> getProperties() {
		return new StandardEdgeQuery(this).getProperties();
	}


	@Override
	public Iterator<Property> getPropertyIterator() {
		return new StandardEdgeQuery(this).getPropertyIterator();
	}
	
	
	@Override
	public Iterable<Property> getProperties(PropertyType type) {
		return new StandardEdgeQuery(this).withEdgeType(type).getProperties();
	}

	@Override
	public Iterable<Property> getProperties(String type) {
        if (!tx.containsEdgeType(type)) return IterablesUtil.emptyIterable();
		else return getProperties(tx.getPropertyType(type));
	}

	@Override
	public Iterator<Property> getPropertyIterator(PropertyType type) {
		return new StandardEdgeQuery(this).withEdgeType(type).getPropertyIterator();
	}
	
	@Override
	public Iterator<Property> getPropertyIterator(String type) {
        if (!tx.containsEdgeType(type)) return Iterators.emptyIterator();
        else return getPropertyIterator(tx.getPropertyType(type));
	}
	
	@Override
	public Iterable<Relationship> getRelationships() {
		return new StandardEdgeQuery(this).getRelationships();
	}


	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		return new StandardEdgeQuery(this).inDirection(dir).getRelationships();
	}


	@Override
	public Iterator<Relationship> getRelationshipIterator() {
		return new StandardEdgeQuery(this).getRelationshipIterator();
	}


	@Override
	public Iterator<Relationship> getRelationshipIterator(Direction dir) {
		return new StandardEdgeQuery(this).inDirection(dir).getRelationshipIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(RelationshipType edgeType, Direction d) {
		return new StandardEdgeQuery(this).inDirection(d).withEdgeType(edgeType).getRelationshipIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType, Direction d) {
        if (!tx.containsEdgeType(edgeType)) return Iterators.emptyIterator();
        else return getRelationshipIterator(tx.getRelationshipType(edgeType),d);
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType,Direction d) {
		return new StandardEdgeQuery(this).inDirection(d).withEdgeType(edgeType).getRelationships();
	}

	@Override
	public Iterable<Relationship> getRelationships(String edgeType,Direction d) {
        if (!tx.containsEdgeType(edgeType)) return IterablesUtil.emptyIterable();
        else return getRelationships(tx.getRelationshipType(edgeType),d);
	}
	
	@Override
	public Iterator<Relationship> getRelationshipIterator(RelationshipType edgeType) {
		return new StandardEdgeQuery(this).withEdgeType(edgeType).getRelationshipIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType) {
        if (!tx.containsEdgeType(edgeType)) return Iterators.emptyIterator();
        else return getRelationshipIterator(tx.getRelationshipType(edgeType));
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType) {
		return new StandardEdgeQuery(this).withEdgeType(edgeType).getRelationships();
	}
	
	@Override
	public Iterable<Relationship> getRelationships(String edgeType) {
        if (!tx.containsEdgeType(edgeType)) return IterablesUtil.emptyIterable();
        else return getRelationships(tx.getRelationshipType(edgeType));
	}
	
	@Override
	public Iterator<Edge> getEdgeIterator() {
		return new StandardEdgeQuery(this).getEdgeIterator();
	}


	@Override
	public Iterator<Edge> getEdgeIterator(Direction dir) {
		return new StandardEdgeQuery(this).inDirection(dir).getEdgeIterator();
	}


	@Override
	public Iterable<Edge> getEdges() {
		return new StandardEdgeQuery(this).getEdges();
	}


	@Override
	public Iterable<Edge> getEdges(Direction dir) {
		return new StandardEdgeQuery(this).inDirection(dir).getEdges();
	}
	
	@Override
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group) {
		return new StandardEdgeQuery(this).withEdgeTypeGroup(group).getRelationships();
	}
	
	@Override
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group, Direction dir) {
		return new StandardEdgeQuery(this).withEdgeTypeGroup(group).inDirection(dir).getRelationships();
	}
	
	@Override
	public Iterable<Property> getProperties(EdgeTypeGroup group) {
		return new StandardEdgeQuery(this).withEdgeTypeGroup(group).getProperties();
	}
	
	@Override
	public Iterable<Property> getProperties(EdgeTypeGroup group, Direction dir) {
		return new StandardEdgeQuery(this).withEdgeTypeGroup(group).inDirection(dir).getProperties();
	}
	
	/* ---------------------------------------------------------------
	 * Edge Counts
	 * ---------------------------------------------------------------
	 */


	@Override
	public int getNoProperties() {
		return new StandardEdgeQuery(this).noProperties();
	}

	@Override
	public int getNoEdges() {
		return getNoProperties() + getNoRelationships();
	}


	@Override
	public int getNoRelationships() {
		return new StandardEdgeQuery(this).noRelationships();
	}
	
	@Override
	public boolean isConnected() {
		return getNoRelationships()>0;
	}

	
	/* ---------------------------------------------------------------
	 * Convenience Methods for Entity Creation
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public Property createProperty(PropertyType relType, Object attribute) {
		return tx.createProperty(relType, this, attribute);
	}


	@Override
	public Property createProperty(String relType, Object attribute) {
		return tx.createProperty(relType, this, attribute);
	}


	@Override
	public Relationship createRelationship(RelationshipType relType, Node node) {
		return tx.createRelationship(relType, this, node);
	}


	@Override
	public Relationship createRelationship(String relType, Node node) {
		return tx.createRelationship(relType, this, node);
	}
	
	/* ---------------------------------------------------------------
	 * Unsupported
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public<T,U> void sendQuery(T queryLoad, Class<? extends QueryType<T,U>> queryType, ResultCollector<U> resultCollector) {
		throw new UnsupportedOperationException("Query-sending is only supported for ReferenceNodes.");
	}
	
	@Override
	public void forwardQuery(Object queryLoad) {
		throw new UnsupportedOperationException("Query-sending is only supported for ReferenceNodes.");
	}

	
	/* ---------------------------------------------------------------
	 * In Memory Entity
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		return InMemoryEntity.instance.getID();
	}

	@Override
	public boolean hasID() {
		return InMemoryEntity.instance.hasID();
	}
	
	@Override
	public void setID(long id) {
		InMemoryEntity.instance.setID(id);
	}

	


	@Override
	public boolean isAccessible() {
		return tx.isOpen();
	}

	@Override
	public boolean isAvailable() {
		return InMemoryEntity.instance.isAvailable();
	}

	@Override
	public boolean isDeleted() {
		return InMemoryEntity.instance.isDeleted();
	}

	@Override
	public boolean isLoaded() {
		return InMemoryEntity.instance.isLoaded();
	}

	@Override
	public boolean isModified() {
		return InMemoryEntity.instance.isModified();
	}

	@Override
	public boolean isNew() {
		return InMemoryEntity.instance.isNew();
	}

	@Override
	public boolean isReferenceNode() {
		return InMemoryEntity.instance.isReferenceNode();
	}



	
	
}
