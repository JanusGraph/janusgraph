package com.thinkaurelius.titan.graphdb.types.group;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TypeGroup;

public class StandardTypeGroup extends TypeGroup {

	public String name;
	public short id;
	
	public StandardTypeGroup() {}
	
	public StandardTypeGroup(short id, String name) {
        Preconditions.checkArgument(id >= 0, "Id must be bigger than 0");
        Preconditions.checkArgument(name!=null);
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
		else if (!(other instanceof TypeGroup)) return false;
		return id==((TypeGroup)other).getID();
	}
	
}
