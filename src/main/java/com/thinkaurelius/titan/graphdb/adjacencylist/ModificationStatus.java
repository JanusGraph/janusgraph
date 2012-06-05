package com.thinkaurelius.titan.graphdb.adjacencylist;

public class ModificationStatus {

	public static final ModificationStatus none = new ModificationStatus() {
		@Override
		public boolean hasChanged() {
			throw new UnsupportedOperationException("Cannot query modification status");
		}
	};
	
	private boolean modified;
	
	public ModificationStatus() {
		modified = false;
	}
	
	void change() {
		modified = true;
	}
	
	void nochange() {
		modified=false;
	}
	
	void setModified(boolean mod) {
		modified=mod;
	}
	
	public boolean hasChanged() {
		return modified;
	}
	
	public void reset() {
		modified = false;
	}
	
}
