package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import static com.tinkerpop.blueprints.Direction.*;

public class InlineTitanEdge extends SimpleTitanEdge implements InlineRelation {

	public InlineTitanEdge(TitanLabel type, InternalTitanVertex start,
                           InternalTitanVertex end) {
		super(type, start, end);
		Preconditions.checkArgument(type.isUnidirected(),"Inline Relationships must be unidirected!");
	}
	
	@Override
	public InlineRelation clone() {
		return new InlineTitanEdge(getTitanLabel(), getVertex(0), getVertex(1));
	}

	@Override
	public boolean isInline() {
		return true;
	}
	
	@Override
	public boolean isAvailable() {
		return getVertex(OUT).isAvailable();
	}
	
	@Override
	public boolean isAccessible() {
		return getVertex(OUT).isAccessible();
	}

	@Override
	public boolean isRemoved() {
		return getVertex(OUT).isRemoved();
	}

	@Override
	public boolean isLoaded() {
		return getVertex(OUT).isLoaded();
	}

	@Override
	public boolean isModified() {
		return getVertex(OUT).isModified();
	}

	@Override
	public boolean isNew() {
		return getVertex(OUT).isNew();
	}


}
