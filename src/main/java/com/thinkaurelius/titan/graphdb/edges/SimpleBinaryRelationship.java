package com.thinkaurelius.titan.graphdb.edges;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Collection;

public class SimpleBinaryRelationship extends AbstractTypedEdge implements Relationship {

	private final InternalNode start;
	private final InternalNode end;
	
	public SimpleBinaryRelationship(RelationshipType type, InternalNode start, InternalNode end) {
		super(type);
		this.start= start;
		this.end = end;
	}
	
	@Override
	public GraphTx getTransaction() {
		return start.getTransaction();
	}
	
	@Override
	public int hashCode() {
		if (isUndirected()) {
			return new HashCodeBuilder().append(start.hashCode()+end.hashCode()).append(type).toHashCode();
		} else {
			assert isDirected() || isUnidirected();
			return new HashCodeBuilder().append(start).append(end).append(type).toHashCode();
		}
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof Relationship)) return false;
		Relationship other = (Relationship)oth;
		if (!getEdgeType().equals(other.getEdgeType())) return false;
		if (isUndirected()) {
			return (start.equals(other.getStart()) && end.equals(other.getEnd()))
			|| (start.equals(other.getEnd()) && end.equals(other.getStart()));
		} else {
			assert isDirected() || isUnidirected();
			return start.equals(other.getStart()) && end.equals(other.getEnd());
		}
	}


	@Override
	public InternalNode getNodeAt(int pos) {
		switch(pos) {
		case 0: return start;
		case 1: return end;
		default: throw new ArrayIndexOutOfBoundsException("Exceeded number of vertices of 2 with given position: " + pos);
		}
	}

	@Override
	public boolean isSelfLoop(Node node) {
		if (!start.equals(end)) return false;
		else return node==null || node.equals(start);
	}

	@Override
	public int getArity() {
		return 2;
	}

	@Override
	public Direction getDirection(Node n) {
		if (isDirected()) {
			if (start.equals(n) && end.equals(n)) return Direction.Both;
			if (start.equals(n)) return Direction.Out;
			else if (end.equals(n)) return Direction.In;
			else throw new InvalidNodeException("Edge is not incident on given node.");
		} else return Direction.Undirected;
	}

	@Override
	public Collection<? extends Node> getNodes() {
		return ImmutableList.of(start,end);
	}

	@Override
	public int getMultiplicity(Node n) {
		return (start.equals(n)?1:0) + (end.equals(n)?1:0);
	}

	@Override
	public NodePosition getPosition(Node n) {
		if (start.equals(n) && end.equals(n)) throw new InvalidNodeException("The specified node occurs multiple times in the relationship!");
		else if (start.equals(n)) return NodePosition.Start;
		else if (end.equals(n)) return NodePosition.End;
		else throw new InvalidNodeException("Edge is not incident on given node.");
	}

	@Override
	public boolean isIncidentOn(Node n) {
		return start.equals(n) || end.equals(n);
	}

	@Override
	public void forceDelete() {
		start.deleteEdge(this);
		if (!isUnidirected())
			end.deleteEdge(this);
		super.forceDelete();
	}

	@Override
	public Node getEnd() {
		return end;
	}

	@Override
	public Node getOtherNode(Node n) {
		if (start.equals(n)) return end;
		else if (end.equals(n)) return start;
		else throw new InvalidNodeException("Edge is not incident on given node.");
	}

	@Override
	public Node getStart() {
		return start;
	}

	@Override
	public RelationshipType getRelationshipType() {
		return (RelationshipType)type;
	}

	@Override
	public final boolean isProperty() {
		return false;
	}

	@Override
	public final boolean isRelationship() {
		return true;
	}

	@Override
	public String toString() {
		return start.toString() + " - " + getRelationshipType().getName() + " -> " + end.toString();
	}



}
