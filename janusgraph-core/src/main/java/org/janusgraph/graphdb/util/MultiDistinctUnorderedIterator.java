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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class MultiDistinctUnorderedIterator<E extends Element> extends CloseableAbstractIterator<E> {

    private final Set<Object> allElements = new HashSet<>();
    private final CloseableIterator<E> iterator;
    private final int limit;
    private long count;

    public MultiDistinctUnorderedIterator(final int lowLimit, final int highLimit, final List<Iterator<E>> iterators) {
        Objects.requireNonNull(iterators);
        iterator = CloseableIteratorUtils.concat(iterators);
        limit = highLimit;

        long i = 0;
        while (i < lowLimit && hasNext()) {
            next();
            i++;
        }
    }

    @Override
    protected E computeNext() {
        if (count < limit) {
            while (iterator.hasNext()) {
                E elem = iterator.next();
                if (allElements.add(elem.id())) {
                    count++;
                    return elem;
                }
            }
        }
        close();
        return endOfData();
    }

    @Override
    public void close() {
        iterator.close();
    }
}
