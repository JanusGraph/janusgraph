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

package org.janusgraph.graphdb.util;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CloseableIteratorUtilsTest {
    @Test
    public void testFilter() {
        MutableBoolean closed = new MutableBoolean(false);
        Iterator<Integer> raw = IntStream.range(0, 10).iterator();
        CloseableIterator<Integer> unfiltered = new CloseableIterator<Integer>() {
            @Override
            public boolean hasNext() {
                return raw.hasNext();
            }

            @Override
            public Integer next() {
                return raw.next();
            }

            @Override
            public void close() {
                closed.setTrue();
            }
        };
        CloseableIterator iterator = CloseableIteratorUtils.filter(unfiltered, num -> num % 2 == 0);
        for (int i = 0; i < 10; i += 2) {
            assertEquals(i, iterator.next());
        }
        assertFalse(closed.getValue());
        assertFalse(iterator.hasNext());
        assertTrue(closed.getValue());
    }
}
