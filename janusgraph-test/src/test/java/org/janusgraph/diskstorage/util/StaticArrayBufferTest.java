// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.StaticBuffer;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StaticArrayBufferTest {

    private static final Random random = new Random();

    @Test
    public void testBasic() {
        long[] values = {2342342342L, 2342, 0, -1, -214252345234L};
        byte[] array = new byte[values.length * 8];
        for (int i = 0; i < values.length; i++) {
            StaticArrayBuffer.putLong(array, i * 8, values[i]);
        }
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], StaticArrayBuffer.getLong(array, i * 8));
        }
    }

    @Test
    public void testToString() {
        for (int i = 0; i < 100; i++) {
            long l = random.nextLong();
            StaticBuffer b = BufferUtil.getLongBuffer(l);
            StaticArrayBuffer staticArrayBuffer = new StaticArrayBuffer(b);
            assertEquals(String.format("0x%016X", l), staticArrayBuffer.toString());
        }
    }
}
