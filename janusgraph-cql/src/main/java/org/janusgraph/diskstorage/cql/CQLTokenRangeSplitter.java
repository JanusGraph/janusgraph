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

import java.math.BigInteger;

/**
 * Splits the Murmur3 token ring into contiguous, non-overlapping ranges so that a full
 * table scan can be issued as several token-bounded queries that run in parallel.
 */
public final class CQLTokenRangeSplitter {

    private CQLTokenRangeSplitter() {
    }

    /**
     * Splits the Murmur3 token ring into {@code numRanges} contiguous ranges that together
     * cover every Murmur3 token. Each returned element is a {@code [start, end]} pair to be
     * used in a {@code token(key) > start AND token(key) <= end} predicate.
     * <p>
     * The first range starts at {@link Long#MIN_VALUE}; because the Murmur3 partitioner never
     * assigns {@code Long.MIN_VALUE} to a key, a {@code token(key) > Long.MIN_VALUE} lower bound
     * still includes every key, while keeping all ranges uniformly half-open {@code (start, end]}.
     */
    public static long[][] splitMurmur3Ring(int numRanges) {
        if (numRanges < 1) {
            throw new IllegalArgumentException("Number of token ranges must be positive, but was " + numRanges);
        }
        final BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
        final BigInteger span = BigInteger.valueOf(Long.MAX_VALUE).subtract(min); // 2^64 - 1
        final BigInteger divisor = BigInteger.valueOf(numRanges);
        final long[][] ranges = new long[numRanges][2];
        long start = Long.MIN_VALUE;
        for (int i = 0; i < numRanges; i++) {
            final long end = (i == numRanges - 1)
                ? Long.MAX_VALUE
                : min.add(span.multiply(BigInteger.valueOf(i + 1L)).divide(divisor)).longValue();
            ranges[i][0] = start;
            ranges[i][1] = end;
            start = end;
        }
        return ranges;
    }
}
