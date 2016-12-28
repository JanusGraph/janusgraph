package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class WriteBufferUtil {


    public static final WriteBuffer put(WriteBuffer out, byte[] bytes) {
        for (int i=0;i<bytes.length;i++) out.putByte(bytes[i]);
        return out;
    }

    public static final WriteBuffer put(WriteBuffer out, StaticBuffer in) {
        for (int i=0;i<in.length();i++) out.putByte(in.getByte(i));
        return out;
    }

}
