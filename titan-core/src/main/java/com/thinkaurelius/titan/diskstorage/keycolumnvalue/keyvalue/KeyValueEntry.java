package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import java.nio.ByteBuffer;

/**
 * Representation of a (key,value) pair.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */

public class KeyValueEntry {

    private final ByteBuffer key;
    private final ByteBuffer value;

    public KeyValueEntry(ByteBuffer key, ByteBuffer value) {
        assert key != null;
        assert value != null;
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key;
    }


    public ByteBuffer getValue() {
        return value;
    }


}
