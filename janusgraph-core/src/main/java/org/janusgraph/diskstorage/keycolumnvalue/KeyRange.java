// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.StaticBuffer;

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
