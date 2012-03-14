package com.thinkaurelius.titan.graphdb.edges;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Collection;

public class SimpleProperty extends AbstractTypedEdge implements Property {

	private final InternalNode node;
	private final Object attribute;
	
	public SimpleProperty(PropertyType type, InternalNode node, Object attribute) {
		super(type);
		Preconditions.checkNotNull(attribute);
		Preconditions.checkArgument(type.getDataType().isInstance(attribute),"Provided attribute does not match property data type!");
		Preconditions.checkNotNull(node);
		this.node = node;
		this.attribute = attribute;
	}
	
	@Override
	public GraphTx getTransaction() {
		return node.getTransaction();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(node).append(attribute).append(type).toHashCode();
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof Property)) return false;
		Property other = (Property)oth;
		if (!getEdgeType().equals(other.getEdgeType())) return false;
		return node.equals(other.getStart()) && attribute.equals(other.getAttribute());
	}

	@Override
	public InternalNode getNodeAt(int pos) {
		if (pos==0) return node;
		throw new ArrayIndexOutOfBoundsException("Exceeded number of vertices of 1 with given position: " + pos);
	}

	@Override
	public boolean isSelfLoop(Node node) {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public Direction getDirection(Node n) {
		if (node.equals(n)) return Direction.Out;
		else throw new InvalidNodeException("Edge is not incident on given node.");
	}

	@Override
	public Collection<? extends Node> getNodes() {
		return ImmutableList.of(node);
	}
	
	@Override
	public NodePosition getPosition(Node n) {
		if (node.equals(n)) return NodePosition.Start;
		throw new InvalidNodeException("Edge is not incident on given node.");
	}

	@Override
	public boolean isIncidentOn(Node n) {
		return node.equals(n);
	}

	@Override
	public void forceDelete() {
		super.forceDelete();
		node.deleteEdge(this);
	}

	@Override
	public Object getAttribute() {
		return attribute;
	}

	@Override
	public <O> O getAttribute(Class<O> clazz) {
		return clazz.cast(attribute);
	}

	@Override
	public Node getStart() {
		return node;
	}

	@Override
	public PropertyType getPropertyType() {
		return (PropertyType)type;
	}

	@Override
	public Number getNumber() {
		return getAttribute(Number.class);
	}

	@Override
	public String getString() {
		return getAttribute(String.class);
	}

	@Override
	public final boolean isProperty() {
		return true;
	}

	@Override
	public final boolean isRelationship() {
		return false;
	}


	

}
