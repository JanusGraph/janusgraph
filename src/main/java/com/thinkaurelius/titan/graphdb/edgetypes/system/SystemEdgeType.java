package com.thinkaurelius.titan.graphdb.edgetypes.system;

import com.thinkaurelius.titan.core.EdgeCategory;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.LoadedEmptyNode;

public abstract class SystemEdgeType extends LoadedEmptyNode implements InternalNode, InternalEdgeType, EdgeTypeDefinition {

	private final String name;
	private final long id;

	
	SystemEdgeType(String name, long id) {
		this.name=SystemEdgeTypeManager.systemETprefix+name;
		this.id=id;

	}
	
	@Override
	public String getName() {
		return name;
	}

	
	/* ---------------------------------------------------------------
	 * Default System Edge Type (same as SystemRelationshipType)
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean isHidden() {
		return true;
	}


	@Override
	public boolean isModifiable() {
		return false;
	}
	
	@Override
	public EdgeTypeGroup getGroup() {
		return SystemEdgeTypeManager.systemETgroup;
	}

	@Override
	public EdgeCategory getCategory() {
		return EdgeCategory.Simple;
	}

	@Override
	public String[] getKeySignature() {
		return new String[0];
	}

	@Override
	public String[] getCompactSignature() {
		return new String[0];
	}
	
	@Override
	public int getSignatureIndex(EdgeType et) {
		throw new IllegalArgumentException("System EdgeType don't have signatures!");
	}

	@Override
	public boolean hasSignatureEdgeType(EdgeType et) {
		return false;
	}
	
	@Override
	public EdgeTypeDefinition getDefinition() {
		return this;
	}


	@Override
	public long getID() {
		return id;
	}

	@Override
	public boolean hasID() {
		return true;
	}

	@Override
	public void setID(long id) {
		throw new IllegalStateException("System EdgeType has already been assigned an id.");
	}	
	
	@Override
	public GraphTx getTransaction() {
		throw new UnsupportedOperationException("Operation is not supported on SystemEdgeType.");
	}
	
	@Override
	public void delete() {
		throw new UnsupportedOperationException("Operation is not supported on SystemEdgeType.");
	}

	@Override
	public boolean isAccessible() {
		return true;
	}
	
}
