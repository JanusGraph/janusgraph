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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class MultiIterator<E> extends CloseableAbstractIterator<E> {

    private Iterator<CloseableIterator<E>> listIterator;
    private CloseableIterator<E> currentIterator;

    public MultiIterator(final List<CloseableIterator<E>> iterators) {
        Objects.requireNonNull(iterators);
        listIterator = iterators.iterator();
    }

    @Override
    protected E computeNext() {
        while (currentIterator != null || listIterator.hasNext()) {
            if (currentIterator == null) {
                currentIterator = listIterator.next();
            } else if (currentIterator.hasNext()) {
                return currentIterator.next();
            } else {
                currentIterator.close();
                currentIterator = null;
            }
        }
        return endOfData();
    }

    @Override
    public void close() {
        while (currentIterator != null || listIterator.hasNext()) {
            if (currentIterator == null) {
                currentIterator = listIterator.next();
            } else {
                currentIterator.close();
                currentIterator = null;
            }
        }
    }
}
