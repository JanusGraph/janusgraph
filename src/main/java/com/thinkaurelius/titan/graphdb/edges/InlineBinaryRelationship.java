package com.thinkaurelius.titan.graphdb.edges;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class InlineBinaryRelationship extends SimpleBinaryRelationship implements InlineEdge {

	public InlineBinaryRelationship(RelationshipType type, InternalNode start,
			InternalNode end) {
		super(type, start, end);
		Preconditions.checkArgument(type.getDirectionality()==Directionality.Unidirected,"Inline Relationships must be unidirected!");
	}
	
	@Override
	public InlineEdge clone() {
		return new InlineBinaryRelationship(getRelationshipType(),getNodeAt(0),getNodeAt(1));
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
