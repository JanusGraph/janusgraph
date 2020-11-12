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

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StaticArrayEntryTest {
    private static final Random random = new Random();

    @Test
    public void testToString() {
        long l = random.nextLong();
        StaticBuffer b = BufferUtil.getLongBuffer(l);
        int column = b.getInt(0);
        int value = b.getInt(4);
        Entry entry = new StaticArrayEntry(b, 4);

        final String[] split = entry.toString().split("->");
        assertEquals(String.format("0x%08X", column), split[0]);
        assertEquals(String.format("0x%08X", value), split[1]);
    }

    @Test
    public void testRandomLongToStringWithNoValue() {
        testToStringWithNoValue(random.nextLong());
    }

    @Test
    public void testHexLongWithLeadingZeroToStringWithNoValue() {
        testToStringWithNoValue(Long.MAX_VALUE >> 3);
    }

    private void testToStringWithNoValue(long l) {
        StaticBuffer b = BufferUtil.getLongBuffer(l);
        long column = b.getLong(0);
        Entry entry = new StaticArrayEntry(b, 8);

        final String[] split = entry.toString().split("->");
        assertEquals(String.format("0x%016X", column), split[0]);
        assertEquals("[no value]", split[1]);
    }
}
