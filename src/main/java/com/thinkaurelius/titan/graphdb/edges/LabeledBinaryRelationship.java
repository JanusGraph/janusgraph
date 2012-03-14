package com.thinkaurelius.titan.graphdb.edges;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.QueryException;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyList;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.ModificationStatus;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edgequery.StandardEdgeQuery;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.NodeUtil;

import java.util.Iterator;

public class LabeledBinaryRelationship extends SimpleBinaryRelationship {

	protected AdjacencyList outEdges;

	
	public LabeledBinaryRelationship(RelationshipType type, InternalNode start,
			InternalNode end, GraphTx tx, AdjacencyListFactory adjList) {
		super(type, start, end);
		assert type.getCategory()==EdgeCategory.Labeled || type.getCategory()==EdgeCategory.LabeledRestricted;
		assert tx!=null;
		this.tx=tx;
		outEdges = adjList.emptyList();
	}

	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		assert isAvailable();
		Preconditions.checkArgument(e.isIncidentOn(this), "Edge is not incident on this node!");
		Preconditions.checkArgument(e instanceof InlineEdge, "Expected inline edge!");
		Preconditions.checkArgument(e.isSimple() && e.isInline(),"Labeled edge only supports simple, virtual edges!");
		Preconditions.checkArgument((e.isProperty()) || e.isUnidirected(),
				"Labeled edge only supports properties or unidirected relationships");
		Preconditions.checkArgument(e.getNodeAt(0).equals(this),"This node only supports out edges!");
		Preconditions.checkArgument(type.getCategory()==EdgeCategory.Labeled || type.getDefinition().hasSignatureEdgeType(e.getEdgeType()),
				"The EdgeType of the added edge does not match the restrictions on this labeled edge!");
		
		ModificationStatus status = new ModificationStatus();
		synchronized(outEdges) {
			outEdges = outEdges.addEdge(e, e.getEdgeType().isFunctional(),status);
		}
		return status.hasChanged();

	}
	
	@Override
	public Iterable<InternalEdge> getEdges(InternalEdgeQuery query,
			boolean loadRemaining) {
		if (!query.isAllowedDirection(EdgeDirection.Out)) return AdjacencyList.Empty;
		else return NodeUtil.filterQueryQualifications(query, NodeUtil.getQuerySpecificIterable(outEdges, query));
	}
	
	@Override
	public void deleteEdge(InternalEdge e) {
		assert isAccessible() && e.isIncidentOn(this) && e.getDirection(this)==Direction.Out;
		outEdges.removeEdge(e,ModificationStatus.none);
	}

	@Override
	public void forceDelete() {
		super.forceDelete();
	}

	
	@Override
	public void loadedEdges(InternalEdgeQuery query) {
		throw new UnsupportedOperationException("Edge loading is not supported on labeled edges!");
	}

	@Override
	public boolean hasLoadedEdges(InternalEdgeQuery query) {
		return true;
	}
	
	/* ---------------------------------------------------------------
	 * ###### The rest is copied verbatim from AbstractNode ##########
	 * ######### copied everything but "Changing Edges" section ######
	 * ---------------------------------------------------------------
	 */
	
	protected final GraphTx tx;

	
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
			return objectHashCode();
		}
		
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof InternalNode)) return false;
		InternalNode other = (InternalNode)oth;
		return NodeUtil.equalIDs(this, other);
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
		return getAttribute(tx.getPropertyType(type));
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
		return getAttribute(tx.getPropertyType(type),clazz);
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
		return getProperties(tx.getPropertyType(type));
	}

	@Override
	public Iterator<Property> getPropertyIterator(PropertyType type) {
		return new StandardEdgeQuery(this).withEdgeType(type).getPropertyIterator();
	}
	
	@Override
	public Iterator<Property> getPropertyIterator(String type) {
		return getPropertyIterator(tx.getPropertyType(type));
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
		return getRelationshipIterator(tx.getRelationshipType(edgeType),d);
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType,Direction d) {
		return new StandardEdgeQuery(this).inDirection(d).withEdgeType(edgeType).getRelationships();
	}

	@Override
	public Iterable<Relationship> getRelationships(String edgeType,Direction d) {
		return getRelationships(tx.getRelationshipType(edgeType),d);
	}
	
	@Override
	public Iterator<Relationship> getRelationshipIterator(RelationshipType edgeType) {
		return new StandardEdgeQuery(this).withEdgeType(edgeType).getRelationshipIterator();
	}

	@Override
	public Iterator<Relationship> getRelationshipIterator(String edgeType) {
		return getRelationshipIterator(tx.getRelationshipType(edgeType));
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType edgeType) {
		return new StandardEdgeQuery(this).withEdgeType(edgeType).getRelationships();
	}
	
	@Override
	public Iterable<Relationship> getRelationships(String edgeType) {
		return getRelationships(tx.getRelationshipType(edgeType));
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


}
