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

package org.janusgraph.graphdb.query;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LimitAdjustingIteratorTest {

    //A backend holding the numbers 1 to size, read under a limit. A read turns each number into an element when it
    //reaches it, and leaves it out if it is in leftOut by then, as the read of an index leaves out the ids of the
    //relations which the transaction removed, before the query as well as while it runs
    private static final class Backend {
        private final int size;
        private final boolean tellsThePosition;
        private final Set<Integer> leftOut = new HashSet<>();
        private final List<Integer> readLimits = new ArrayList<>();
        //Every number a read turned into an element or left out, in that order
        private final List<Integer> converted = new ArrayList<>();

        private Backend(int size, int leftOutUpTo, boolean tellsThePosition) {
            this.size = size;
            this.tellsThePosition = tellsThePosition;
            leftOut.addAll(numbers(1, leftOutUpTo));
        }

        private Iterator<Integer> read(int limit) {
            readLimits.add(limit);
            final Read read = new Read(limit);
            if (tellsThePosition) {
                return read;
            }
            return new Iterator<Integer>() {
                @Override
                public boolean hasNext() {
                    return read.hasNext();
                }

                @Override
                public Integer next() {
                    return read.next();
                }
            };
        }

        private final class Read implements Iterator<Integer>, BackendLimitAware {
            private final int limit;
            private final int returned;
            private int position;
            private Integer next;
            private boolean started;

            private Read(int limit) {
                this.limit = limit;
                this.returned = Math.min(limit, size);
            }

            @Override
            public boolean hasNext() {
                started = true;
                while (next == null && position < returned) {
                    position++;
                    converted.add(position);
                    if (!leftOut.contains(position)) {
                        next = position;
                    }
                }
                return next != null;
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final Integer element = next;
                next = null;
                return element;
            }

            @Override
            public boolean hasBackendReachedLimit() {
                return position >= limit;
            }

            @Override
            public long getBackendResultsRead() {
                return position;
            }

            @Override
            public void skipBackendResults(long count) {
                if (started) {
                    throw new IllegalStateException();
                }
                position = (int) Math.min(count, returned);
            }
        }
    }

    //Takes up to wanted elements, adding each to the backend's left-out ones right away where the reader removes what
    //it reads, as a traversal which drops the edges it reads does
    private static List<Integer> read(Backend backend, int maxLimit, int limit, int wanted,
                                      boolean removesWhatItReads) {
        final LimitAdjustingIterator<Integer> iterator = new LimitAdjustingIterator<Integer>(maxLimit, limit) {
            @Override
            public Iterator<Integer> getNewIterator(int newLimit) {
                return backend.read(newLimit);
            }
        };
        final List<Integer> read = new ArrayList<>();
        while (read.size() < wanted && iterator.hasNext()) {
            final Integer element = iterator.next();
            read.add(element);
            if (removesWhatItReads) {
                backend.leftOut.add(element);
            }
        }
        return read;
    }

    private static List<Integer> readAll(Backend backend, int maxLimit, int limit) {
        return read(backend, maxLimit, limit, Integer.MAX_VALUE, false);
    }

    private static List<Integer> numbers(int from, int to) {
        return IntStream.rangeClosed(from, to).boxed().collect(Collectors.toList());
    }

    @Test
    public void shouldReadOnPastWhatTheReadsLeaveOut() {
        final Backend backend = new Backend(30, 8, true);
        assertEquals(numbers(9, 30), readAll(backend, Integer.MAX_VALUE - 1, 10));
        assertEquals(Arrays.asList(10, 20, 40), backend.readLimits);
        //A read under a larger limit skips what the reads before went through, rather than turning it into elements
        assertEquals(numbers(1, 30), backend.converted);
    }

    @Test
    public void shouldReadOnWhileAReadKeepsNothing() {
        final Backend backend = new Backend(30, 25, true);
        assertEquals(numbers(26, 30), readAll(backend, Integer.MAX_VALUE - 1, 5));
        assertEquals(Arrays.asList(5, 10, 20, 40), backend.readLimits);
        assertEquals(numbers(1, 30), backend.converted);
    }

    @Test
    public void shouldEndOnceTheBackendRunsOutWhereEveryResultIsLeftOut() {
        final Backend backend = new Backend(30, 30, true);
        assertEquals(Collections.emptyList(), readAll(backend, Integer.MAX_VALUE - 1, 10));
        assertEquals(Arrays.asList(10, 20, 40), backend.readLimits);
    }

    @Test
    public void shouldNotReadBeyondTheMaxLimit() {
        final Backend backend = new Backend(30, 20, true);
        assertEquals(Collections.emptyList(), readAll(backend, 16, 8));
        assertEquals(Arrays.asList(8, 16), backend.readLimits);
    }

    //A backend which holds exactly as many results as the limit is read once more, which finds nothing
    @Test
    public void shouldReadOnceMoreWhereTheBackendHoldsExactlyTheLimit() {
        final Backend backend = new Backend(10, 0, true);
        assertEquals(numbers(1, 10), readAll(backend, Integer.MAX_VALUE - 1, 10));
        assertEquals(Arrays.asList(10, 20), backend.readLimits);
        assertEquals(numbers(1, 10), backend.converted);
    }

    //A reader which removes what it reads, between the reads, makes the elements of the read under a larger limit
    //fewer than the elements returned so far: skipping as many of them would throw, or skip some it never returned
    @Test
    public void shouldReturnEveryElementOnceWhereTheReaderRemovesWhatItReadsAfterAShortRead() {
        final Backend backend = new Backend(15, 8, true);
        assertEquals(numbers(9, 15), read(backend, Integer.MAX_VALUE - 1, 15, 10, true));
        assertEquals(Arrays.asList(15, 30), backend.readLimits);
    }

    @Test
    public void shouldReturnTheFirstElementsWhereTheReaderRemovesWhatItReadsAfterAShortRead() {
        final Backend backend = new Backend(30, 8, true);
        assertEquals(numbers(9, 18), read(backend, Integer.MAX_VALUE - 1, 15, 10, true));
        assertEquals(Arrays.asList(15, 30), backend.readLimits);
    }

    @Test
    public void shouldReturnEveryElementOnceWhereTheReaderRemovesWhatItReadsAfterAFullRead() {
        final Backend backend = new Backend(30, 0, true);
        assertEquals(numbers(1, 30), read(backend, Integer.MAX_VALUE - 1, 10, Integer.MAX_VALUE, true));
        assertEquals(Arrays.asList(10, 20, 40), backend.readLimits);
        assertEquals(numbers(1, 30), backend.converted);
    }

    //An iterator which doesn't tell whether the backend reached the limit ends the result at a short read, as before
    @Test
    public void shouldTakeAShortReadForTheEndWhereTheIteratorDoesNotTellTheLimit() {
        final Backend backend = new Backend(30, 8, false);
        assertEquals(numbers(9, 10), readAll(backend, Integer.MAX_VALUE - 1, 10));
        assertEquals(Collections.singletonList(10), backend.readLimits);
    }

    //And it is iterated through as many elements as were returned when a read returns the whole limit, as before
    @Test
    public void shouldSkipTheElementsReturnedWhereTheIteratorDoesNotTellThePosition() {
        final Backend backend = new Backend(30, 0, false);
        assertEquals(numbers(1, 30), readAll(backend, Integer.MAX_VALUE - 1, 10));
        assertEquals(Arrays.asList(10, 20, 40), backend.readLimits);
        //1 to 10, then 1 to 20, then 1 to 30
        assertEquals(60, backend.converted.size());
    }
}
