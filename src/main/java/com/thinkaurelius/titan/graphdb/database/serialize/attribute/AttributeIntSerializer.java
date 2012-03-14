package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeInt;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;

import java.nio.ByteBuffer;

public class AttributeIntSerializer implements AttributeSerializer<AttributeInt> {

	private static final long serialVersionUID = 1174998819862504186L;

	@Override
	public AttributeInt read(ByteBuffer buffer) {
		return new AttributeInt(buffer.getInt()+Integer.MIN_VALUE);
	}

	@Override
	public void writeObjectData(ByteBuffer buffer, AttributeInt object) {
		buffer.putInt(object.getIntValue()-Integer.MIN_VALUE);		
	}

}
