package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

public class KeyRange {
    public final StaticBuffer start;
    public final StaticBuffer end;

    public KeyRange(StaticBuffer start, StaticBuffer end) {
        this.start = start;
        this.end = end;
    }

    public String toString() {
        return String.format("KeyRange(left: %s, right: %s)", start, end);
    }
}
