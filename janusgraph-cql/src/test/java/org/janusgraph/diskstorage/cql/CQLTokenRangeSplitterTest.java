// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CQLTokenRangeSplitterTest {

    @Test
    public void singleRangeCoversWholeRing() {
        long[][] ranges = CQLTokenRangeSplitter.splitMurmur3Ring(1);
        assertEquals(1, ranges.length);
        assertEquals(Long.MIN_VALUE, ranges[0][0]);
        assertEquals(Long.MAX_VALUE, ranges[0][1]);
    }

    @Test
    public void rangesTileTheRingContiguouslyWithoutGapsOrOverlaps() {
        for (int n : new int[]{2, 3, 4, 8, 17, 256}) {
            long[][] ranges = CQLTokenRangeSplitter.splitMurmur3Ring(n);
            assertEquals(n, ranges.length, "range count for n=" + n);
            assertEquals(Long.MIN_VALUE, ranges[0][0], "first start for n=" + n);
            assertEquals(Long.MAX_VALUE, ranges[n - 1][1], "last end for n=" + n);
            for (int i = 0; i < n; i++) {
                assertTrue(ranges[i][0] < ranges[i][1], "start<end for range " + i + " n=" + n);
                if (i > 0) {
                    // contiguous and non-overlapping: each range starts exactly where the previous ended,
                    // and the predicate is (start, end], so the shared boundary belongs to exactly one range.
                    assertEquals(ranges[i - 1][1], ranges[i][0], "contiguity at " + i + " n=" + n);
                }
            }
        }
    }

    @Test
    public void boundariesAreEvenlySpacedAcrossTheRing() {
        // For an even split the boundaries should be the evenly spaced points of the 2^64 ring.
        long[][] ranges = CQLTokenRangeSplitter.splitMurmur3Ring(2);
        // midpoint of [-2^63, 2^63-1] lands at -1
        assertEquals(-1L, ranges[0][1]);
        assertEquals(-1L, ranges[1][0]);

        long[][] quarters = CQLTokenRangeSplitter.splitMurmur3Ring(4);
        BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger span = BigInteger.valueOf(Long.MAX_VALUE).subtract(min);
        for (int i = 1; i < 4; i++) {
            long expected = min.add(span.multiply(BigInteger.valueOf(i)).divide(BigInteger.valueOf(4))).longValue();
            assertEquals(expected, quarters[i - 1][1], "boundary " + i);
        }
    }

    @Test
    public void rejectsNonPositiveRangeCount() {
        assertThrows(IllegalArgumentException.class, () -> CQLTokenRangeSplitter.splitMurmur3Ring(0));
        assertThrows(IllegalArgumentException.class, () -> CQLTokenRangeSplitter.splitMurmur3Ring(-3));
    }
}
