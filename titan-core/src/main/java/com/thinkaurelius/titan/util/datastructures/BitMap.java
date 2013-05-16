package com.thinkaurelius.titan.util.datastructures;

/**
 * Utility class for setting and reading individual bits in a byte.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BitMap {

    public final static byte createMapb(int pos) {
        assert pos >= 0 && pos < 8;
        return (byte) (1 << pos);
    }

    public final static byte setBitb(byte map, int pos) {
        assert pos >= 0 && pos < 8;
        return (byte) (map | (1 << pos));
    }

    public final static byte unsetBitb(byte map, int pos) {
        assert pos >= 0 && pos < 8;
        return (byte) (map & ~(1 << pos));
    }

    public final static boolean readBitb(byte map, int pos) {
        assert pos >= 0 && pos < 8;
        return ((map >>> pos) & 1) == 1;
    }

}
