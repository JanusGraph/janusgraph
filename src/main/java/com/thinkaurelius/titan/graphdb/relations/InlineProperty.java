package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public class InlineProperty extends SimpleProperty implements InlineRelation {

	public InlineProperty(TitanKey type, InternalTitanVertex node,
			Object attribute) {
		super(type, node, attribute);
	}
	
	
	@Override
	public InlineRelation clone() {
		return new InlineProperty(getPropertyKey(), getVertex(0),getAttribute());
	}

	@Override
	public boolean isInline() {
		return true;
	}
	
	@Override
	public boolean isAvailable() {
		return getVertex().isAvailable();
	}
	
	@Override
	public boolean isAccessible() {
		return getVertex().isAccessible();
	}

	@Override
	public boolean isRemoved() {
		return getVertex().isRemoved();
	}

	@Override
	public boolean isLoaded() {
		return getVertex().isLoaded();
	}

	@Override
	public boolean isModified() {
		return getVertex().isModified();
	}

	@Override
	public boolean isNew() {
		return getVertex().isNew();
	}




}
