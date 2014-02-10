package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utility methods for dealing with {@link ByteBuffer}.
 *
 */
public class BufferUtil {

    public static final int longSize = StaticArrayBuffer.LONG_LEN;
    public static final int intSize = StaticArrayBuffer.INT_LEN;

    /* ###############
     * Simple StaticBuffer construction
     * ################
     */

    public static final StaticBuffer getIntBuffer(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize);
        buffer.putInt(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize);
        return StaticArrayBuffer.of(arr);
    }

    public static final StaticBuffer getIntBuffer(int[] ids) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize * ids.length);
        for (int i = 0; i < ids.length; i++)
            buffer.putInt(ids[i]);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize * ids.length);
        return StaticArrayBuffer.of(arr);
    }

    public static final StaticBuffer getLongBuffer(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(longSize);
        buffer.putLong(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == longSize);
        return StaticArrayBuffer.of(arr);
    }


    public static final StaticBuffer fillBuffer(int len, byte value) {
        byte[] res = new byte[len];
        for (int i = 0; i < len; i++) res[i]=value;
        return StaticArrayBuffer.of(res);
    }

    public static final StaticBuffer oneBuffer(int len) {
        return fillBuffer(len,(byte)-1);
    }

    public static final StaticBuffer zeroBuffer(int len) {
        return fillBuffer(len,(byte)0);
    }

    public static final StaticBuffer emptyBuffer() {
        return fillBuffer(0,(byte)0);
    }

    /* ################
     * StaticBuffer Manipulation
     * ################
     */


    public static final StaticBuffer nextBiggerBufferAllowOverflow(StaticBuffer buffer) {
        return nextBiggerBuffer(buffer, true);
    }

    public static final StaticBuffer nextBiggerBuffer(StaticBuffer buffer) {
        return nextBiggerBuffer(buffer,false);
    }

    private static final StaticBuffer nextBiggerBuffer(StaticBuffer buffer, boolean allowOverflow) {
        int len = buffer.length();
        byte[] next = new byte[len];
        boolean carry = true;
        for (int i = len - 1; i >= 0; i--) {
            byte b = buffer.getByte(i);
            if (carry) {
                b++;
                if (b != 0) carry = false;
            }
            next[i]=b;
        }
        if (!allowOverflow) {
            Preconditions.checkArgument(!carry, "Buffer overflow");
            return StaticArrayBuffer.of(next);
        } else {
            return zeroBuffer(len);
        }

    }

    /**
     * Thread safe equals method for StaticBuffer - ByteBuffer equality comparison
     *
     * @param b1
     * @param b2
     * @return
     */
    public static final boolean equals(StaticBuffer b1, ByteBuffer b2) {
        if (b1.length()!=b2.remaining()) return false;
        int p2 = b2.position();
        for (int i=0;i<b1.length();i++) {
            if (b1.getByte(i)!=b2.get(p2+i)) return false;
        }
        return true;
    }


}
