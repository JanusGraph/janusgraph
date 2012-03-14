package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeReal;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;

import java.nio.ByteBuffer;

public class AttributeRealSerializer implements AttributeSerializer<AttributeReal> {

	private static final long serialVersionUID = -1719511496523862718L;

	@Override
	public AttributeReal read(ByteBuffer buffer) {
		long convert = buffer.getLong();
		convert = convert + Long.MIN_VALUE;
		return new AttributeReal(((double)convert)/AttributeReal.getMultiplier());
	}

	@Override
	public void writeObjectData(ByteBuffer buffer, AttributeReal object) {
		long convert = (long)(object.getValue()*AttributeReal.getMultiplier()) - Long.MIN_VALUE;
		buffer.putLong(convert);		
	}

}
