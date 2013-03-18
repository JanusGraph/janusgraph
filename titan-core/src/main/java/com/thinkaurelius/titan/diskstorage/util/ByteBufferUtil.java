package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteBufferUtil {

    public static final int longSize = 8;
    public static final int intSize = 4;

    public static final ByteBuffer getIntByteBuffer(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize);
        buffer.putInt(id);
        buffer.flip();
        return buffer;
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
        assert buffer.position() == 0;
        int len = buffer.remaining();
        ByteBuffer next = ByteBuffer.allocate(len);
        boolean carry = true;
        for (int i = len - 1; i >= 0; i--) {
            byte b = buffer.get(i);
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

    /**
     * Compares two {@link java.nio.ByteBuffer}s and checks whether the first ByteBuffer is smaller than the second.
     *
     * @param a First ByteBuffer
     * @param b Second ByteBuffer
     * @return true if the first ByteBuffer is smaller than the second
     */
    public static final boolean isSmallerThan(ByteBuffer a, ByteBuffer b) {
        return isSmallerThanWithEqual(a, b, false);
    }

    /**
     * Compares two {@link java.nio.ByteBuffer}s and checks whether the first ByteBuffer is smaller than or equal to the second.
     *
     * @param a First ByteBuffer
     * @param b Second ByteBuffer
     * @return true if the first ByteBuffer is smaller than or equal to the second
     */
    public static final boolean isSmallerOrEqualThan(ByteBuffer a, ByteBuffer b) {
        return isSmallerThanWithEqual(a, b, true);
    }

    /**
     * Compares two {@link java.nio.ByteBuffer}s.
     * <p/>
     * If considerEqual is true, it checks whether the first ByteBuffer is smaller than or equal to the second.
     * If considerEqual is false, it checks whether the first ByteBuffer is smaller than the second.
     *
     * @param a             First ByteBuffer
     * @param b             Second ByteBuffer
     * @param considerEqual Determines comparison mode
     * @return true if the first ByteBuffer is smaller than (or equal to) the second
     */
    public static final boolean isSmallerThanWithEqual(ByteBuffer a, ByteBuffer b, boolean considerEqual) {
        if (a == b) {
            return considerEqual;
        }
        a.mark();
        b.mark();
        boolean result = true;
        while (true) {
            if (!a.hasRemaining() && b.hasRemaining()) break;
            else if (a.hasRemaining() && b.hasRemaining()) {
                byte ca = a.get(), cb = b.get();
                if (ca != cb) {
                    if (ca >= 0 && cb >= 0) {
                        if (ca < cb) break;
                        else if (ca > cb) {
                            result = false;
                            break;
                        }
                    } else if (ca < 0 && cb < 0) {
                        if (ca < cb) break;
                        else if (ca > cb) {
                            result = false;
                            break;
                        }
                    } else if (ca >= 0 && cb < 0) break;
                    else {
                        result = false;
                        break;
                    }
                }
            } else if (a.hasRemaining() && !b.hasRemaining()) {
                result = false;
                break;
            } else { //!a.hasRemaining() && !b.hasRemaining()
                result = considerEqual;
                break;
            }
        }
        a.reset();
        b.reset();
        return result;
    }

    public static final String toBitString(ByteBuffer b, String byteSeparator) {
        StringBuilder s = new StringBuilder();
        while (b.hasRemaining()) {
            byte n = b.get();
            String bn = Integer.toBinaryString(n);
            if (bn.length() > 8) bn = bn.substring(bn.length() - 8);
            else if (bn.length() < 8) {
                while (bn.length() < 8) bn = "0" + bn;
            }
            s.append(bn).append(byteSeparator);
        }
        b.rewind();
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

        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + buffer.position();
            if (boff == 0 && length == buffer.array().length)
                return buffer.array();
            else
                return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }
}
