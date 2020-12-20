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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class CloseableIteratorUtils {

    public static final CloseableIterator<Object> EMPTY_CLOSABLE_ITERATOR = CloseableIterator.asCloseable(Collections.emptyIterator());

    public static <T> CloseableIterator<T> emptyIterator() {
        return (CloseableIterator<T>) EMPTY_CLOSABLE_ITERATOR;
    }

    public static <T> CloseableIterator<T> concat(final Iterator<T>... iterators) {
        return concat(Arrays.asList(iterators));
    }

    public static <T> CloseableIterator<T> concat(final List<Iterator<T>> iterators) {
        return new MultiIterator<>(iterators);
    }

    public static <T> CloseableIterator<T> filter(final Iterator<T> unfiltered,
                                                  final Predicate<? super T> retainIfTrue) {
        Objects.requireNonNull(unfiltered);
        Objects.requireNonNull(retainIfTrue);
        return new CloseableAbstractIterator<T>() {
            @Override
            protected T computeNext() {
                while (unfiltered.hasNext()) {
                    T element = unfiltered.next();
                    if (retainIfTrue.test(element)) {
                        return element;
                    }
                }
                close();
                return endOfData();
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(unfiltered);
            }
        };
    }
}
