package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

import java.util.Iterator;

public abstract class LoadedEmptyNode implements InternalNode {

	private static final String errorName = "Empty Nodes";
	
	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges!");
	}
	
	@Override
	public void deleteEdge(InternalEdge e) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges!");
	}

	
	@Override
	public void loadedEdges(InternalEdgeQuery query) {
		throw new UnsupportedOperationException(errorName + " do not support incident edges!");
	}

	@Override
	public boolean hasLoadedEdges(InternalEdgeQuery query) {
		return true;
	}

	/* ---------------------------------------------------------------
	 * Edge Iteration/Access
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public EdgeQuery edgeQuery() {
		throw new UnsupportedOperationException(errorName + " do not support querying!");
	}

	@Override
	public Object getAttribute(PropertyType type) {
		return null;
	}
	
	@Override
	public<O> O getAttribute(PropertyType type, Class<O> clazz) {
		return null;
	}
	
	@Override
	public String getString(PropertyType type) {
		return null;
	}
	
	@Override
	public Number getNumber(PropertyType type) {
		return null;
	}
	
	@Override
	public Object getAttribute(String type) {
		return null;
	}

	@Override
	public <O> O getAttribute(String type, Class<O> clazz) {
		return null;
	}

	@Override
	public Number getNumber(String type) {
		return null;
	}

	@Override
	public String getString(String type) {
		return null;
	}


	@Override
	public Iterable<InternalEdge> getEdges(InternalEdgeQuery query,
			boolean loadRemaining) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterable<Property> getProperties() {
		return IterablesUtil.emptyIterable();
	}


	@Override
	public Iterator<Property> getPropertyIterator() {
		return Iterators.emptyIterator();
	}
	
	
	@Override
	public Iterable<Property> getProperties(PropertyType type) {
		return IterablesUtil.emptyIterable();
	}


	@Override
	public Iterator<Property> getPropertyIterator(PropertyType type) {
		return Iterators.emptyIterator();
	}
	
	
	@Override
	public Iterable<Relationship> getRelationships() {
		return IterablesUtil.emptyIterable();
	}


	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		return IterablesUtil.emptyIterable();
	}


	@Override
	public Iterator<Relationship> getRelationshipIterator() {
		return Iterators.emptyIterator();
	}


	@Override
	public Iterator<Relationship> getRelationshipIterator(Direction dir) {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(RelationshipType edgeType, Direction d) {
		return Iterators.emptyIterator();
	}


	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType,Direction d) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterator<Relationship> getRelationshipIterator(RelationshipType edgeType) {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterable<Property> getProperties(String propType) {
		return IterablesUtil.emptyIterable();
	}

	@Override
	public Iterator<Property> getPropertyIterator(String propType) {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType,
			Direction d) {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType) {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterable<Relationship> getRelationships(String edgeType, Direction d) {
		return IterablesUtil.emptyIterable();
	}

	@Override
	public Iterable<Relationship> getRelationships(String edgeType) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterator<Edge> getEdgeIterator() {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterator<Edge> getEdgeIterator(Direction dir) {
		return Iterators.emptyIterator();
	}

	@Override
	public Iterable<Edge> getEdges() {
		return IterablesUtil.emptyIterable();
	}

	@Override
	public Iterable<Edge> getEdges(Direction dir) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group, Direction dir) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterable<Property> getProperties(EdgeTypeGroup group) {
		return IterablesUtil.emptyIterable();
	}
	
	@Override
	public Iterable<Property> getProperties(EdgeTypeGroup group, Direction dir) {
		return IterablesUtil.emptyIterable();
	}
	
	/* ---------------------------------------------------------------
	 * Edge Counts
	 * ---------------------------------------------------------------
	 */


	@Override
	public int getNoProperties() {
		return 0;
	}

	@Override
	public int getNoEdges() {
		return 0;
	}


	@Override
	public int getNoRelationships() {
		return 0;
	}
	
	@Override
	public boolean isConnected() {
		return false;
	}


	
	/* ---------------------------------------------------------------
	 * Convenience Methods for Entity Creation
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public Property createProperty(PropertyType relType, Object attribute) {
		throw new UnsupportedOperationException(errorName + " do not support incident properties!");
	}


	@Override
	public Property createProperty(String relType, Object attribute) {
		throw new UnsupportedOperationException(errorName + " do not support incident properties!");
	}


	@Override
	public Relationship createRelationship(RelationshipType relType, Node node) {
		throw new UnsupportedOperationException(errorName + " do not support incident relationships!");
	}


	@Override
	public Relationship createRelationship(String relType, Node node) {
		throw new UnsupportedOperationException(errorName + " do not support incident relationships!");
	}

	
	/* ---------------------------------------------------------------
	 * In Memory Entity
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		throw new UnsupportedOperationException(errorName + " don't have an ID!");
	}

	@Override
	public boolean hasID() {
		return false;
	}
	
	@Override
	public void setID(long id) {
		throw new UnsupportedOperationException(errorName + " don't have an ID!");
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
	public boolean isDeleted() {
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
	public boolean isReferenceNode() {
		return false;
	}
	
}
