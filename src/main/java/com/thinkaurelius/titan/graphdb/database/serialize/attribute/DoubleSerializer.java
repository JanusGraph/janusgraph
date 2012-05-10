package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;

public class DoubleSerializer implements AttributeSerializer<Double> {

	private static final long serialVersionUID = -1719511496523862718L;

    public static final int DECIMALS = 6;
    private static int MULTIPLIER = 0;
    
    static {
        MULTIPLIER =1;
        for (int i=0;i<DECIMALS;i++) MULTIPLIER *=10;
    }
    
	@Override
	public Double read(ByteBuffer buffer) {
		long convert = buffer.getLong();
		convert = convert + Long.MIN_VALUE;
		return Double.valueOf(((double)convert)/ MULTIPLIER);
	}

	@Override
	public void writeObjectData(ByteBuffer buffer, Double object) {
		long convert = (long)(object.doubleValue()*MULTIPLIER) - Long.MIN_VALUE;
		buffer.putLong(convert);		
	}

}
