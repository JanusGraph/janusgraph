package com.thinkaurelius.titan.graphdb.edges;

import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class InlineProperty extends SimpleProperty implements InlineEdge {

	public InlineProperty(PropertyType type, InternalNode node,
			Object attribute) {
		super(type, node, attribute);
	}
	
	
	@Override
	public InlineEdge clone() {
		return new InlineProperty(getPropertyType(),getNodeAt(0),getAttribute());
	}

	@Override
	public boolean isInline() {
		return true;
	}
	
	@Override
	public boolean isAvailable() {
		return getStart().isAvailable();
	}
	
	@Override
	public boolean isAccessible() {
		return getStart().isAccessible();
	}

	@Override
	public boolean isDeleted() {
		return getStart().isDeleted();
	}

	@Override
	public boolean isLoaded() {
		return getStart().isLoaded();
	}

	@Override
	public boolean isModified() {
		return getStart().isModified();
	}

	@Override
	public boolean isNew() {
		return getStart().isNew();
	}




}
