package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utility methods for dealing with {@link ByteBuffer}.
 *
 */
public class ByteBufferUtil {

    public static final int longSize = 8;
    public static final int intSize = 4;

    public static final ByteBuffer getIntByteBuffer(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize);
        buffer.putInt(id);
        buffer.flip();
        return buffer;
    }

    public static final StaticBuffer getIntBuffer(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize);
        buffer.putInt(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize);
        return new StaticArrayBuffer(arr);
    }

    public static final StaticBuffer getIntBuffer(int[] ids) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize * ids.length);
        for (int i = 0; i < ids.length; i++)
            buffer.putInt(ids[i]);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize * ids.length);
        return new StaticArrayBuffer(arr);
    }

    public static final StaticBuffer getLongBuffer(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(longSize);
        buffer.putLong(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == longSize);
        return new StaticArrayBuffer(arr);
    }

    public static final ByteBuffer getLongByteBuffer(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(longSize);
        buffer.putLong(id);
        buffer.flip();
        return buffer;
    }

    public static final ByteBuffer getLongByteBuffer(long[] ids) {
        ByteBuffer buffer = ByteBuffer.allocate(longSize * ids.length);
        for (int i = 0; i < ids.length; i++)
            buffer.putLong(ids[i]);
        buffer.flip();
        return buffer;
    }

    public static final ByteBuffer nextBiggerBuffer(ByteBuffer buffer) {
        int len = buffer.remaining();
        int pos = buffer.position();
        ByteBuffer next = ByteBuffer.allocate(len);
        boolean carry = true;
        for (int i = len - 1; i >= 0; i--) {
            byte b = buffer.get(i+pos);
            if (carry) {
                b++;
                if (b != 0) carry = false;
            }
            next.put(i, b);
        }
        Preconditions.checkArgument(!carry, "Buffer overflow");
        next.position(0);
        next.limit(len);
        return next;
    }

    public static final StaticBuffer nextBiggerBuffer(StaticBuffer buffer) {
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
        Preconditions.checkArgument(!carry, "Buffer overflow");
        return new StaticArrayBuffer(next);
    }

    public static final ByteBuffer zeroByteBuffer(int len) {
        ByteBuffer res = ByteBuffer.allocate(len);
        for (int i = 0; i < len; i++) res.put((byte) 0);
        res.flip();
        return res;
    }

    public static final ByteBuffer oneByteBuffer(int len) {
        ByteBuffer res = ByteBuffer.allocate(len);
        for (int i = 0; i < len; i++) res.put((byte) -1);
        res.flip();
        return res;
    }

    public static final StaticBuffer fillBuffer(int len, byte value) {
        byte[] res = new byte[len];
        for (int i = 0; i < len; i++) res[i]=value;
        return new StaticArrayBuffer(res);
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

    /**
     * Compares two {@link java.nio.ByteBuffer}s and checks whether the first ByteBuffer is smaller than the second.
     *
     * @param a First ByteBuffer
     * @param b Second ByteBuffer
     * @return true if the first ByteBuffer is smaller than the second
     */
    public static final boolean isSmallerThan(ByteBuffer a, ByteBuffer b) {
        return compare(a, b)<0;
    }

    /**
     * Compares two {@link java.nio.ByteBuffer}s and checks whether the first ByteBuffer is smaller than or equal to the second.
     *
     * @param a First ByteBuffer
     * @param b Second ByteBuffer
     * @return true if the first ByteBuffer is smaller than or equal to the second
     */
    public static boolean isSmallerOrEqualThan(ByteBuffer a, ByteBuffer b) {
        return compare(a,b)<=0;
    }
    
    /**
     * Compares two {@link StaticBuffer}s and checks whether the first {@code StaticBuffer} is smaller than the second.
     *
     * @param a First StaticBuffer
     * @param b Second StaticBuffer
     * @return true if the first StaticBuffer is smaller than the second
     */
    public static boolean isSmallerThan(StaticBuffer a, StaticBuffer b) {
        return compare(a, b) < 0;
    }

    /**
     * Compares two {@link StaticBuffer}s and checks whether the first {@code StaticBuffer} is smaller than or equal to the second.
     *
     * @param a First StaticBuffer
     * @param b Second StaticBuffer
     * @return true if the first StaticBuffer is smaller than or equal to the second
     */
    public static boolean isSmallerOrEqualThan(StaticBuffer a, StaticBuffer b) {
        return compare(a,b)<=0;
    }

    /**
     * Compares two {@link java.nio.ByteBuffer}s according to their byte order (and not the byte value).
     * <p/>
     *
     * @param b1             First ByteBuffer
     * @param b2             Second ByteBuffer
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     */
    public static final int compare(ByteBuffer b1, ByteBuffer b2) {
        if (b1 == b2) {
            return 0;
        }
        int p1=b1.position(), p2 = b2.position();
        while (p1<b1.limit() || p2<b2.limit()) {
            if (p1>=b1.limit()) return -1;
            else if (p2>=b2.limit()) return 1;
            else {
                int cmp = compare(b1.get(p1), b2.get(p2));
                if (cmp!=0) return cmp;
            }
            p1++; p2++;
        }
        return 0; //Must be equal
    }

    public static int compare(byte c1, byte c2) {
        int a = c1 & 0xff;
        int b = c2 & 0xff;

        return a - b;
    }

    public static int compare(StaticBuffer b1, StaticBuffer b2) {
        if (b1 == b2) {
            return 0;
        }

        // fast path for byte array comparison
        if ((b1 instanceof StaticArrayBuffer) && (b2 instanceof StaticArrayBuffer))
            return ((StaticArrayBuffer) b1).compareTo((StaticArrayBuffer) b2);

        int i, j;
        for (i = 0, j = 0; i < b1.length() && j < b2.length(); i++, j++)
        {
            int cmp = compare(b1.getByte(i), b2.getByte(j));
            if (cmp != 0)
                return cmp;
        }

        // equivalent of b1.remaining() - b2.remaining()
        return (b1.length() - i + 1) - (b2.length() - j + 1);
    }

    /**
     * Thread-safe hashcode method for ByteBuffer written according to Effective
     * Java 2e by Josh Bloch.
     * 
     * @param b ByteBuffer
     * @return hashcode for given ByteBuffer
     */
    public static final int hashcode(ByteBuffer b) {
        int result = 17;
        for (int i = b.position(); i < b.limit(); i++) {
            result = 31 * result + (int)b.get(i);
        }
        return result;
    }

    /**
     * Thread-safe hashcode method for StaticBuffer written according to
     * Effective Java 2e by Josh Bloch.
     * 
     * @param b ByteBuffer
     * @return hashcode for given StaticBuffer
     */
    public static final int hashcode(StaticBuffer b) {
        int result = 17;
        for (int i = 0; i < b.length(); i++) {
            result = 31 * result + (int)b.getByte(i);
        }
        return result;
    }

    /**
     * Thread safe equals method for ByteBuffers
     *
     * @param b1
     * @param b2
     * @return
     */
    public static final boolean equals(ByteBuffer b1, ByteBuffer b2) {
        if (b1.remaining()!=b2.remaining()) return false;
        int p1 = b1.position(), p2 = b2.position();
        while (p1<b1.limit() && p2<b2.limit()) {
            if (b1.get(p1)!=b2.get(p2)) return false;
            p1++; p2++;
        }
        assert p1==b1.limit() && p2==b2.limit();
        return true;
    }

    /**
     * Thread safe equals method for StaticBuffers
     *
     * @param b1
     * @param b2
     * @return
     */
    public static final boolean equals(StaticBuffer b1, StaticBuffer b2) {
        if (b1.length()!=b2.length()) return false;
        for (int i=0;i<b1.length();i++) {
            if (b1.getByte(i)!=b2.getByte(i)) return false;
        }
        return true;
    }

    public static final String toString(ByteBuffer b, String separator) {
        StringBuilder s = new StringBuilder();
        for (int i=b.position();i<b.limit();i++) {
            if (i>b.position()) s.append(separator);
            byte c = b.get(i);
            if (c>=0) s.append(c);
            else s.append(256+c);
        }
        return s.toString();
    }

    public static final String toString(StaticBuffer b, String separator) {
        StringBuilder s = new StringBuilder();
        for (int i=0;i<b.length();i++) {
            if (i>0) s.append(separator);
            byte c = b.getByte(i);
            if (c>=0) s.append(c);
            else s.append(256+c);
        }
        return s.toString();
    }


    public static final String toBitString(ByteBuffer b, String byteSeparator) {
        StringBuilder s = new StringBuilder();
        for (int i=b.position();i<b.limit();i++) {
            byte n = b.get(i);
            String bn = Integer.toBinaryString(n);
            if (bn.length() > 8) bn = bn.substring(bn.length() - 8);
            else if (bn.length() < 8) {
                while (bn.length() < 8) bn = "0" + bn;
            }
            s.append(bn).append(byteSeparator);
        }
        return s.toString();
    }

    public static String bytesToHex(ByteBuffer bytes) {
        final int offset = bytes.position();
        final int size = bytes.remaining();
        final char[] c = new char[size * 2];
        for (int i = 0; i < size; i++) {
            final int bint = bytes.get(i + offset);
            c[i * 2] = Hex.byteToChar[(bint & 0xf0) >> 4];
            c[1 + i * 2] = Hex.byteToChar[bint & 0x0f];
        }
        return Hex.wrapCharArray(c);
    }

    public static byte[] getArray(ByteBuffer buffer)
    {
        int length = buffer.remaining();

        if (buffer.hasArray()) {
            int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        } else {
            byte[] bytes = new byte[length];
            int pos = 0;
            for (int i=buffer.position();i<buffer.limit();i++) {
                bytes[pos]=buffer.get(i);
                pos++;
            }
            Preconditions.checkArgument(pos == length);
            return bytes;
        }
    }
}
