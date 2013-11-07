package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KeyRange {

    private final StaticBuffer start;
    private final StaticBuffer end;

    public KeyRange(StaticBuffer start, StaticBuffer end) {
        this.start = start;
        this.end = end;
    }

    public StaticBuffer getAt(int position) {
        switch(position) {
            case 0: return start;
            case 1: return end;
            default: throw new IndexOutOfBoundsException("Exceed length of 2: " + position);
        }
    }

    public StaticBuffer getStart() {
        return start;
    }

    public StaticBuffer getEnd() {
        return end;
    }
}
