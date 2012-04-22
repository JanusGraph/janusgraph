package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

import java.util.Iterator;

public class StandardReferenceNode implements InternalNode {

	protected final GraphTx tx;
	protected final long nodeid;
	
	public StandardReferenceNode(GraphTx tx, long nodeid) {
		this.tx=tx;
		this.nodeid=nodeid;
	}

	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		return true;
	}
	
	@Override
	public GraphTx getTransaction() {
		return tx;
	}

	@Override
	public void deleteEdge(InternalEdge e) {
		//Do nothing
	}

	@Override
	public Iterable<InternalEdge> getEdges(InternalEdgeQuery query,
			boolean loadRemaining) {
		throw new UnsupportedOperationException("Cannot query edges on this node.");
	}



	@Override
	public boolean hasLoadedEdges(InternalEdgeQuery query) {
		throw new UnsupportedOperationException("Cannot query edges on this node.");
	}

	@Override
	public void loadedEdges(InternalEdgeQuery query) {
		throw new UnsupportedOperationException("Cannot load edges on this node.");
	}

	@Override
	public Property createProperty(PropertyType relType, Object attribute) {
		throw new UnsupportedOperationException("Cannot create edges on this node.");
	}

	@Override
	public Property createProperty(String relType, Object attribute) {
		throw new UnsupportedOperationException("Cannot create edges on a ReferenceNode");
	}

	@Override
	public Relationship createRelationship(RelationshipType relType, Node node) {
		throw new UnsupportedOperationException("Cannot create edges on a ReferenceNode");
	}

	@Override
	public Relationship createRelationship(String relType, Node node) {
		throw new UnsupportedOperationException("Cannot create edges on a ReferenceNode");
	}

	@Override
	public EdgeQuery edgeQuery() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Object getAttribute(PropertyType type) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public <O> O getAttribute(PropertyType type, Class<O> clazz) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public int getNoProperties() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public int getNoEdges() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public int getNoRelationships() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public boolean isConnected() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Number getNumber(PropertyType type) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterable<Property> getProperties() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterable<Property> getProperties(PropertyType propType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Property> getPropertyIterator() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Property> getPropertyIterator(PropertyType propType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(
			RelationshipType edgeType, Direction d) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(Direction dir) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(
			RelationshipType edgeType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType,
			Direction d) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterable<Relationship> getRelationships() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Iterable<Property> getProperties(String propType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Property> getPropertyIterator(String propType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType,
			Direction d) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}


	@Override
	public Iterable<Relationship> getRelationships(String edgeType, Direction d) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}


	@Override
	public Iterable<Relationship> getRelationships(String edgeType) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Iterator<Edge> getEdgeIterator() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}


	@Override
	public Iterator<Edge> getEdgeIterator(Direction dir) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}


	@Override
	public Iterable<Edge> getEdges() {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group, Direction dir) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Iterable<Property> getProperties(EdgeTypeGroup group) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Iterable<Property> getProperties(EdgeTypeGroup group, Direction dir) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}


	@Override
	public Iterable<Edge> getEdges(Direction dir) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public String getString(PropertyType type) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}
	
	@Override
	public Object getAttribute(String type) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public <O> O getAttribute(String type, Class<O> clazz) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public Number getNumber(String type) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public String getString(String type) {
		throw new UnsupportedOperationException("Cannot query edges on a ReferenceNode");
	}

	@Override
	public void delete() {
		throw new UnsupportedOperationException("Cannot modify a ReferenceNode");
	}

	@Override
	public long getID() {
		return nodeid;
	}

	@Override
	public boolean hasID() {
		return true;
	}

	@Override
	public boolean isAccessible() {
		return tx.isOpen();
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public boolean isDeleted() {
		return false;
	}

	@Override
	public boolean isLoaded() {
		return false;
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
		return true;
	}

	@Override
	public void setID(long id) {
		throw new UnsupportedOperationException("Cannot modify a ReferenceNode");
	}






	
}
