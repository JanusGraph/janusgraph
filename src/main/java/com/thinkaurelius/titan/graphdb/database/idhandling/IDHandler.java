package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IDHandler {
    
    public final static ByteBuffer getKey(long id) {
        assert id>=0;
        return ByteBufferUtil.getLongByteBuffer(id<<1);
    }
    
    public final static long getKeyID(ByteBuffer b) {
        long value = b.getLong();
        return value>>>1;
    }


    
}
