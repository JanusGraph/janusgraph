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

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiIteratorTest {

    @Test
    public void shouldConcatEmptyIterators() {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            iterators.add(Collections.emptyIterator());
        }
        MultiIterator<Integer> multiIterator = new MultiIterator<>(iterators);
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }

    @Test
    public void shouldConcatSingleIterator() {
        List<Iterator<String>> iterators = new LinkedList<>();
        iterators.add(Arrays.asList("value1", "value2").iterator());
        MultiIterator<String> multiIterator = new MultiIterator<>(iterators);
        assertEquals("value1", multiIterator.next());
        assertEquals("value2", multiIterator.next());
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }

    @Test
    public void shouldConcatIterators() {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        final int num = 3;
        for (int i = 0; i < num; i++) {
            iterators.add(Collections.emptyIterator());
            iterators.add(Arrays.asList(i).iterator());
        }
        MultiIterator<Integer> multiIterator = new MultiIterator<>(iterators);
        for (int i = 0; i < num; i++) {
            assertEquals(i, multiIterator.next());
        }
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }

    @Test
    public void shouldCloseUnderlyingIteratorsAfterIteration() {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        final int num = 3;
        List<Boolean> closed = initialise(iterators, num);

        MultiIterator<Integer> multiIterator = new MultiIterator<>(iterators);
        for (int i = 0; i < num; i++) {
            // iterator shall be closed as long as it is exhausted
            assertEquals(i, multiIterator.next());
            if (i > 0) assertTrue(closed.get(i - 1));
        }
        assertFalse(closed.get(num - 1));
        assertFalse(multiIterator.hasNext());
        assertTrue(closed.get(num - 1));
    }

    @Test
    public void shouldCloseUnderlyingIteratorsWhenCloseCalledAtBeginning() {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        final int num = 3;
        List<Boolean> closed = initialise(iterators, num);

        MultiIterator<Integer> multiIterator = new MultiIterator<>(iterators);
        multiIterator.close();

        for (int i = 0; i < num; i++) {
            assertTrue(closed.get(i), "Iterator #" + i + " is not closed");
        }
    }

    @Test
    public void shouldCloseUnderlyingIteratorsWhenCloseCalledInTheMiddle() {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        final int num = 3;
        List<Boolean> closed = initialise(iterators, num);

        MultiIterator<Integer> multiIterator = new MultiIterator<>(iterators);
        assertEquals(0, multiIterator.next());
        assertEquals(1, multiIterator.next());
        multiIterator.close();

        for (int i = 0; i < num; i++) {
            assertTrue(closed.get(i), "Iterator #" + i + " is not closed");
        }
    }

    /**
     * To verify that `computeNext` is never called and NPE is not thrown.
     */
    @Test
    public void shouldNotHasNextWhenIteratorIsEmpty() {
        MultiIterator<Integer> multiIterator = new MultiIterator<>(Collections.emptyList());
        assertFalse(multiIterator.hasNext());
        assertDoesNotThrow(multiIterator::close);
    }

    /**
     * To verify that `computeNext` is never called and NPE is not thrown.
     */
    @Test
    public void shouldThrowNoSuchElementExceptionWhenNextCalledWithEmptyIterators() {
        MultiIterator<Integer> multiIterator = new MultiIterator<>(Collections.emptyList());
        assertThrows(NoSuchElementException.class, multiIterator::next);
        assertDoesNotThrow(multiIterator::close);
    }

    private List<Boolean> initialise(List<Iterator<Integer>> iterators, int num) {
        List<Boolean> closed = Arrays.asList(false, false, false);
        for (int i = 0; i < num; i++) {
            Iterator<Integer> iter = Arrays.asList(i).iterator();
            int finalI = i;
            iterators.add(new CloseableIterator<Integer>() {
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public Integer next() {
                    return iter.next();
                }

                @Override
                public void close() {
                    closed.set(finalI, true);
                }
            });
        }
        return closed;
    }
}
