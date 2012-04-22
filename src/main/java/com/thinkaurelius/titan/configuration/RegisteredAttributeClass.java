package com.thinkaurelius.titan.configuration;

import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;

import java.io.Serializable;

class RegisteredAttributeClass<T> implements Serializable {

	private static final long serialVersionUID = -1410497942890545395L;

	final Class<T> type;
	final AttributeSerializer<T> serializer;
	
	RegisteredAttributeClass(Class<T> type) {
		this(type,null);
	}
	
	RegisteredAttributeClass(Class<T> type, AttributeSerializer<T> serializer) {
		this.type=type;
		this.serializer=serializer;
	}
	
	void registerWith(Serializer s) {
		if (serializer==null) s.registerClass(type);
		else s.registerClass(type, serializer);
	}
	
	public boolean equals(Object oth) {
		if (this==oth) return true;
		else if (!getClass().isInstance(oth)) return false;
		return type.equals(((RegisteredAttributeClass<?>)oth).type);
	}
	
	
}
