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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BitMapTest {

    @Test
    public void testBitMap() {
        byte map = BitMap.setBitb(BitMap.createMapb(2), 4);
        assertTrue(BitMap.readBitb(map, 2));
        assertTrue(BitMap.readBitb(map, 4));
        map = BitMap.unsetBitb(map, 2);
        assertFalse(BitMap.readBitb(map, 2));
        assertFalse(BitMap.readBitb(map, 3));
        assertFalse(BitMap.readBitb(map, 7));
        map = BitMap.setBitb(map, 7);
        assertTrue(BitMap.readBitb(map, 7));
    }

}
