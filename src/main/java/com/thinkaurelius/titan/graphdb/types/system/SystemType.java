package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.graphdb.types.TypeCategory;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.LoadedEmptyTitanVertex;

public abstract class SystemType extends LoadedEmptyTitanVertex implements InternalTitanVertex, InternalTitanType, TypeDefinition {

	private final String name;
	private final long id;

	
	SystemType(String name, long id) {
		this.name= SystemTypeManager.systemETprefix+name;
		this.id=id;

	}
	
	@Override
	public String getName() {
		return name;
	}

	
	/* ---------------------------------------------------------------
	 * Default System TitanRelation Type (same as SystemLabel)
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
	public TypeGroup getGroup() {
		return SystemTypeManager.SYSTEM_TYPE_GROUP;
	}

	@Override
	public TypeCategory getCategory() {
		return TypeCategory.Simple;
	}

    @Override
    public boolean isSimple() {
        return true;
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
	public TypeDefinition getDefinition() {
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
		throw new IllegalStateException("SystemType has already been assigned an id");
	}	
	
	@Override
	public InternalTitanTransaction getTransaction() {
		throw new UnsupportedOperationException("Operation is not supported on SystemType");
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Operation is not supported on SystemType");
	}

	@Override
	public boolean isAccessible() {
		return true;
	}
	
}
