package com.thinkaurelius.titan.util.datastructures;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ByteSize {

    public static final int OBJECT_HEADER = 12;

    public static final int OBJECT_REFERENCE = 8;

    public static final int GUAVA_CACHE_ENTRY_SIZE = 104;

    //Does not include array contents of byte[]
    public static final int STATICARRAYBUFFER_RAW_SIZE = OBJECT_HEADER + 2*4 + 6 + (OBJECT_REFERENCE + OBJECT_HEADER + 8); // 6 = overhead & padding, (byte[] array)

    public static final int ARRAYLIST_SIZE = OBJECT_HEADER + 4 + OBJECT_REFERENCE + OBJECT_HEADER + 6; // 4 = size, 6=padding


}
