package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class WriteBufferUtil {


    public static final WriteBuffer put(WriteBuffer out, byte[] bytes) {
        for (int i=0;i<bytes.length;i++) out.putByte(bytes[i]);
        return out;
    }

}
