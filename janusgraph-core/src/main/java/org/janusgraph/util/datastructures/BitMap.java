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

package org.janusgraph.util.datastructures;

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
