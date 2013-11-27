package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import java.util.List;

import com.thinkaurelius.titan.core.AttributeHandler;

public class ByteArrayHandler implements AttributeHandler<byte[]> {

    @Override
    public void verifyAttribute(byte[] value) {
        //All values are valid
    }

    @Override
    public byte[] convert(Object value) {
        if(value instanceof List) {
        	if(((List)value).get(0) instanceof Byte) {
	            List<Byte> byteList = (List<Byte>)value;
	            byte[] buff = new byte[byteList.size()];
	            for(int i=0; i<byteList.size(); i++) {
	                buff[i] = byteList.get(i);
	            }
	            return buff;
        	}
        } else if (value instanceof byte[]) {
        	return (byte[])value;
        }
        return null;
    }
}