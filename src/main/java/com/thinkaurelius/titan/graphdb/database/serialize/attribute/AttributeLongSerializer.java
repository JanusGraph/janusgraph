package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeLong;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;

import java.nio.ByteBuffer;

public class AttributeLongSerializer implements AttributeSerializer<AttributeLong> {

	private static final long serialVersionUID = -8438674418838450877L;

	@Override
	public AttributeLong read(ByteBuffer buffer) {
		return new AttributeLong(buffer.getLong()+Long.MIN_VALUE);
	}

	@Override
	public void writeObjectData(ByteBuffer buffer, AttributeLong object) {
		buffer.putLong(object.getLongValue()-Long.MIN_VALUE);		
	}

}
