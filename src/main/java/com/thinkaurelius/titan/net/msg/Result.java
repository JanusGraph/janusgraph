package com.thinkaurelius.titan.net.msg;

import java.nio.ByteBuffer;

/**
 *
 * @author dalaro
 */
public class Result extends Message {

    private final Key seed;
    private final ByteBuffer[] data;
    private final int dataBytes;

    public Result(Key seed, ByteBuffer[] data) {
        this.seed = seed;
        this.data = data;
        int i = 0;
        for (ByteBuffer b : data) {
        	if (null == b)
        		continue;
            i += b.remaining();
        }
        this.dataBytes = i;
    }


    public Key getSeed() {
        return seed;
    }

    public ByteBuffer[] getData() {
        return data;
    }

    public int getDataByteCount() {
        return dataBytes;
    }

    public byte[] getDataByteArray() {
        int size = 0;
        for (ByteBuffer bb : getData()) {
        	if (null == bb)
        		continue;
            size += bb.remaining();
        }

        byte[] result = new byte[size];
        ByteBuffer target = ByteBuffer.wrap(result);

        for (ByteBuffer bb : getData()) {
        	if (null == bb)
        		continue;
            bb.mark();
            target.put(bb);
            bb.reset();
        }

        return result;
    }

    @Override
    public String toString() {
        return "Result[" +
                "queryKey=" + seed + "," +
                "dataBytes=" + dataBytes + ']';
    }
}
