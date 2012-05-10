package com.thinkaurelius.titan.graphdb.configuration;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;

import java.io.Serializable;

public class RegisteredAttributeClass<T> implements Comparable<RegisteredAttributeClass>{

    private final Class<T> type;
	private final AttributeSerializer<T> serializer;
    private final int position;
	
	public RegisteredAttributeClass(Class<T> type, int position) {
		this(type,null,position);
	}
	
	public RegisteredAttributeClass(Class<T> type, AttributeSerializer<T> serializer, int position) {
		this.type=type;
		this.serializer=serializer;
        this.position=position;
	}
	
	void registerWith(Serializer s) {
		if (serializer==null) s.registerClass(type);
		else s.registerClass(type, serializer);
	}
	
    @Override
	public boolean equals(Object oth) {
		if (this==oth) return true;
		else if (!getClass().isInstance(oth)) return false;
		return type.equals(((RegisteredAttributeClass<?>)oth).type) || position==((RegisteredAttributeClass)oth).position;
	}

    @Override
    public String toString() {
        return type.toString() + "#" + position;
    }
    
    @Override
    public int compareTo(RegisteredAttributeClass registeredAttributeClass) {
        return position - registeredAttributeClass.position;
    }
}
