package com.thinkaurelius.titan.graphdb.types;

public enum TypeVisibility {

	Hidden ,
	
	Unmodifiable ,
	
	Modifiable;
	
	public boolean isHidden() {
		switch(this) {
		case Hidden: return true;
		case Unmodifiable:
		case Modifiable: return false;
		default: throw new AssertionError("Unexpected enum constant: " + this);
		}
	}
	
	public boolean isModifiable() {
		switch(this) {
		case Hidden: 
		case Unmodifiable: return false;
		case Modifiable: return true;
		default: throw new AssertionError("Unexpected enum constant: " + this);
		}
	}
	
}
