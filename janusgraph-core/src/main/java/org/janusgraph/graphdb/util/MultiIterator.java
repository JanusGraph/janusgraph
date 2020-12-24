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

import java.util.Collection;
import java.util.Iterator;

public class MultiIterator<E> extends CloseableAbstractIterator<E> {

    private final Iterator<Iterator<E>> collectionIterator;
    private Iterator<E> currentIterator;

    /**
     * @param iterators iterators to concatenate. Must be not null. During iteration, iterators are closed lazily when
     *                  exhausted (if an iterator extends {@link AutoCloseable}).
     */
    public MultiIterator(final Collection<Iterator<E>> iterators) {
        collectionIterator = iterators.iterator();
        if(collectionIterator.hasNext()){
            currentIterator = collectionIterator.next();
        } else {
            endOfData();
        }
    }

    @Override
    protected E computeNext() {
        while (!currentIterator.hasNext() && collectionIterator.hasNext()){
            CloseableIterator.closeIterator(currentIterator);
            currentIterator = collectionIterator.next();
        }
        if(currentIterator.hasNext()){
            return currentIterator.next();
        }
        CloseableIterator.closeIterator(currentIterator);
        currentIterator = null;
        return endOfData();
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(currentIterator);
        while (collectionIterator.hasNext()){
            CloseableIterator.closeIterator(collectionIterator.next());
        }
    }
}
