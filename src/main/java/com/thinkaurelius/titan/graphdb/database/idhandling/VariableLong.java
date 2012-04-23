package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLong {

    private static final byte mask = (1<<7)-1;
    private static final byte stopMask = -128;

    public static long readPositive(ByteBuffer in) {
        long value = 0;
        byte b = 0;
        do {
            b = in.get();
            value = value << 7;
            value = value | (b & mask);
        } while (b>=0);
        return value;
    }
    
    public static void writePositive(ByteBuffer out, final long value) {
        Preconditions.checkArgument(value>=0,"Positive value expected: " + value);
        int offset = 63;
        while (offset>0) {
            offset -= 7;
            byte x = mask | stopMask; 
            long cut = value>>offset;
            if (cut>0) {
                byte b= (byte)(cut%128);
                if (offset==0) {
                    b = (byte) (b | stopMask);
                }
                out.put(b);
            }
        }
    }
    
    public static void write(ByteBuffer out, final long value) {
        writePositive(out,value*2 + value<0?1:0);
    }

    public static long read(ByteBuffer in) {
        long value = readPositive(in);
        boolean neg = value%2==1;
        value = value/2;
        if (neg) value = -value;
        return value;
    }


}
