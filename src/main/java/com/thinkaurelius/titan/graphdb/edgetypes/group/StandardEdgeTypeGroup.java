package com.thinkaurelius.titan.graphdb.edgetypes.group;

import com.thinkaurelius.titan.core.EdgeTypeGroup;

public class StandardEdgeTypeGroup extends EdgeTypeGroup {

	public String name;
	public short id;
	
	public StandardEdgeTypeGroup() {}
	
	public StandardEdgeTypeGroup(short id, String name) {
		assert id>=0;
		this.id=id;
		this.name=name;
	}
	
	@Override
	public short getID() {
		return id;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public boolean equals(Object other) {
		if (this==other) return true;
		else if (!(other instanceof EdgeTypeGroup)) return false;
		return id==((EdgeTypeGroup)other).getID();
	}
	
}
