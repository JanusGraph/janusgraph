package com.thinkaurelius.titan.util.stats;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class NumberUtil {

    public static boolean isPowerOf2(long value) {
        return value>0 && Long.highestOneBit(value)==value;
    }

    /**
     * Returns an integer X such that 2^X=value. Throws an exception
     * if value is not a power of 2.
     *
     * @param value
     * @return
     */
    public static int getPowerOf2(long value) {
        Preconditions.checkArgument(isPowerOf2(value));
        return Long.SIZE-(Long.numberOfLeadingZeros(value)+1);
    }

}
