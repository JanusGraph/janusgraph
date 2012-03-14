package com.thinkaurelius.titan.graphdb.vertices;


public abstract class NewEmptyNode extends LoadedEmptyNode {


	@Override
	public boolean isLoaded() {
		return false;
	}

	@Override
	public boolean isNew() {
		return true;
	}
	
}
