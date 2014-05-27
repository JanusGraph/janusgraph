package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * A range of bytes between start and end where start is inclusive and end is exclusive.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KeyRange {

    private final StaticBuffer start;
    private final StaticBuffer end;

    public KeyRange(StaticBuffer start, StaticBuffer end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return String.format("KeyRange(left: %s, right: %s)", start, end);
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
