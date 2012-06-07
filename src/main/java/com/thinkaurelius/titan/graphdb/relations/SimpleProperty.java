package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class SimpleProperty extends AbstractTypedRelation implements TitanProperty {

	private final InternalTitanVertex node;
	private final Object attribute;
	
	public SimpleProperty(TitanKey type, InternalTitanVertex node, Object attribute) {
		super(type);
		Preconditions.checkNotNull(attribute);
        if (!type.getDataType().equals(Object.class)) {
		    Preconditions.checkArgument(type.getDataType().equals(attribute.getClass()),"Provided attribute does not match property data type!");
        }
		Preconditions.checkNotNull(node);
		this.node = node;
		this.attribute = attribute;
	}
	
	@Override
	public InternalTitanTransaction getTransaction() {
		return node.getTransaction();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(node).append(attribute).append(type).toHashCode();
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof TitanProperty)) return false;
		TitanProperty other = (TitanProperty)oth;
		if (!getType().equals(other.getType())) return false;
		return node.equals(other.getVertex()) && attribute.equals(other.getAttribute());
	}
    
    @Override
    public String toString() {
        return "p" + StringFactory.L_BRACKET + getId() + StringFactory.R_BRACKET +
                StringFactory.L_BRACKET + getVertex().getId() + StringFactory.DASH +
                getPropertyKey().getName() + StringFactory.ARROW + getAttribute() + StringFactory.R_BRACKET;
    }

	@Override
	public InternalTitanVertex getVertex(int pos) {
		if (pos==0) return node;
		throw new ArrayIndexOutOfBoundsException("Exceeded number of vertices of 1 with given position: " + pos);
	}

	@Override
	public boolean isLoop() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public Direction getDirection(TitanVertex vertex) {
		if (node.equals(vertex)) return Direction.OUT;
		else throw new InvalidElementException("TitanRelation is not incident on given node",vertex);
	}

	@Override
	public boolean isIncidentOn(TitanVertex vertex) {
		return node.equals(vertex);
	}

	@Override
	public synchronized void forceDelete() {
		super.forceDelete();
		node.removeRelation(this);
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
	public TitanVertex getVertex() {
		return node;
	}

	@Override
	public TitanKey getPropertyKey() {
		return (TitanKey)type;
	}

	@Override
	public final boolean isProperty() {
		return true;
	}

	@Override
	public final boolean isEdge() {
		return false;
	}


	

}
