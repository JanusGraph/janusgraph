// Copyright 2019 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.function.MultiComparator;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder.OrderEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class MultiDistinctOrderedIterator<E extends Element> implements CloseableIterator<E> {

    private final Map<Integer, Iterator<E>> iterators = new LinkedHashMap<>();
    private final Map<Integer, E> values = new LinkedHashMap<>();
    private final TreeMap<E, Integer> currentElements;
    private final Set<Object> allElements = new HashSet<>();
    private final Integer limit;
    private long count = 0;

    public MultiDistinctOrderedIterator(final Integer lowLimit, final Integer highLimit, final List<Iterator<E>> iterators, final List<OrderEntry> orders) {
        this.limit = highLimit;
        final List<Comparator<E>> comp = new ArrayList<>();
        orders.forEach(o -> comp.add(new ElementValueComparator(o.key, o.order)));
        Comparator<E> comparator = new MultiComparator<>(comp);
        for (int i = 0; i < iterators.size(); i++) {
            this.iterators.put(i, iterators.get(i));
        }
        currentElements = new TreeMap<>(comparator);
        long i = 0;
        while (i < lowLimit && this.hasNext()) {
            this.next();
            i++;
        }
    }

    @Override
    public boolean hasNext() {
        if (limit != null && count >= limit) {
            return false;
        }
        for (int i = 0; i < iterators.size(); i++) {
            if (!values.containsKey(i) && iterators.get(i).hasNext()){
                E element = null;
                do {
                    element = iterators.get(i).next();
                    if (allElements.contains(element.id())) {
                        element = null;
                    }
                } while (element == null && iterators.get(i).hasNext());
                if (element != null) {
                    values.put(i, element);
                    currentElements.put(element, i);
                    allElements.add(element.id());
                }
            }
        }
        return !values.isEmpty();
    }

    @Override
    public E next() {
        count++;
        return values.remove(currentElements.remove(currentElements.firstKey()));
    }

    @Override
    public void close() {
        iterators.values().forEach(CloseableIterator::closeIterator);
    }

}
