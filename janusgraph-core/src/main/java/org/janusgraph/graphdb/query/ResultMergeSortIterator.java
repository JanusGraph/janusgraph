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

package org.janusgraph.graphdb.query;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ResultMergeSortIterator<R> implements CloseableIterator<R> {

    private final Iterator<R> first;
    private final Iterator<R> second;
    private final Comparator<R> comp;
    private final boolean filterDuplicates;

    private R nextFirst;
    private R nextSecond;
    private R next;

    public ResultMergeSortIterator(Iterator<R> first, Iterator<R> second, Comparator<R> comparator, boolean filterDuplicates) {
        Preconditions.checkNotNull(first);
        Preconditions.checkNotNull(second);
        Preconditions.checkNotNull(comparator);
        this.first = first;
        this.second = second;
        this.comp = comparator;
        this.filterDuplicates = filterDuplicates;

        nextFirst = null;
        nextSecond = null;
        next = nextInternal();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public R next() {
        if (!hasNext()) throw new NoSuchElementException();
        R current = next;
        next = null;
        do {
            next = nextInternal();
        } while (next != null && filterDuplicates && comp.compare(current, next) == 0);
        return current;
    }

    public R nextInternal() {
        if (nextFirst == null && first.hasNext()) {
            nextFirst = first.next();
            assert nextFirst != null;
        }
        if (nextSecond == null && second.hasNext()) {
            nextSecond = second.next();
            assert nextSecond != null;
        }
        final R result;
        if (nextFirst == null && nextSecond == null) {
            return null;
        } else if (nextFirst == null) {
            result = nextSecond;
            nextSecond = null;
        } else if (nextSecond == null) {
            result = nextFirst;
            nextFirst = null;
        } else {
            //Compare
            int c = comp.compare(nextFirst, nextSecond);
            if (c <= 0) {
                result = nextFirst;
                nextFirst = null;
            } else {
                result = nextSecond;
                nextSecond = null;
            }
        }
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static<R> Iterable<R> mergeSort(final Iterable<R> first, final Iterable<R> second,
                                           final Comparator<R> comparator, final boolean filterDuplicates) {
        return () -> new ResultMergeSortIterator<>(first.iterator(), second.iterator(), comparator, filterDuplicates);
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(first);
        CloseableIterator.closeIterator(second);
    }
}
