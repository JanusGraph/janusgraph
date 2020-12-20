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

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.janusgraph.core.JanusGraphElement;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wraps around a result set iterator to return up to the specified limit number of elements
 * and implement the {@link java.util.Iterator#remove()} method based on element's remove method.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ResultSetIterator<R extends JanusGraphElement> implements CloseableIterator<R> {

    private final Iterator<R> iterator;
    private final int limit;

    private R current;
    private R next;
    private int count;


    public ResultSetIterator(Iterator<R> inner, int limit) {
        this.iterator = inner;
        this.limit = limit;
        count = 0;

        this.current = null;
        this.next = nextInternal();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    private R nextInternal() {
        R r = null;
        if (count < limit && iterator.hasNext()) {
            r = iterator.next();
        } else {
            close();
        }
        return r;
    }

    @Override
    public R next() {
        if (!hasNext())
            throw new NoSuchElementException();

        current = next;
        count++;
        next = nextInternal();
        return current;
    }

    @Override
    public void remove() {
        if (current != null)
            current.remove();
        else
            throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(iterator);
    }

    public static<R extends JanusGraphElement> Iterable<R> wrap(final Iterable<R> inner, final int limit) {
        return () -> new ResultSetIterator<>(inner.iterator(), limit);
    }

}
