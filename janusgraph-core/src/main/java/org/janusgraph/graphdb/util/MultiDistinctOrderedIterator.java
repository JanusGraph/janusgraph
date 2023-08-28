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
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder.OrderEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

public class MultiDistinctOrderedIterator<E extends Element> extends CloseableAbstractIterator<E> {

    private final Map<Integer, Iterator<E>> iterators;
    private final PriorityQueue<E> queue;
    private final Set<Object> allElements = new HashSet<>();
    private final List<E> iteratorValues;
    private final Map<E, Integer> iteratorIdx;
    private final Integer limit;
    private final boolean singleIterator;
    private long count = 0;

    public MultiDistinctOrderedIterator(final Integer lowLimit, final Integer highLimit, final List<Iterator<E>> iterators, final List<OrderEntry> orders) {
        this.limit = highLimit;
        this.singleIterator = iterators.size() == 1;
        this.iterators = new LinkedHashMap<>(iterators.size());
        this.iteratorValues = new ArrayList<>(iterators.size());
        this.iteratorIdx = new HashMap<>(iterators.size());
        for (int i = 0; i < iterators.size(); i++) {
            this.iterators.put(i, iterators.get(i));
            this.iteratorValues.add(null);
        }
        final List<Comparator<E>> comp = new ArrayList<>();
        orders.forEach(o -> comp.add(new ElementValueComparator(o.key, o.order)));
        this.queue = new PriorityQueue<>(iterators.size(), new MultiComparator<>(comp));
        long i = 0;
        while (i < lowLimit && this.hasNext()) {
            this.next();
            i++;
        }
    }

    @Override
    protected E computeNext() {
        if (limit != null && limit != Query.NO_LIMIT && count >= limit) {
            return endOfData();
        }
        for (int i = 0; i < iterators.size(); i++) {
            if (iteratorValues.get(i) == null && iterators.get(i).hasNext()) {
                E element = null;
                do {
                    element = iterators.get(i).next();
                    if (allElements.contains(element.id())) {
                        element = null;
                    }
                } while (element == null && iterators.get(i).hasNext());
                if (element != null) {
                    iteratorValues.set(i, element);
                    iteratorIdx.put(element, i);
                    queue.add(element);
                    if (!singleIterator) {
                        allElements.add(element.id());
                    }
                }
            }
        }
        if (queue.isEmpty()) {
            return endOfData();
        } else {
            count++;
            E result = queue.remove();
            iteratorValues.set(iteratorIdx.remove(result), null);
            return result;
        }
    }

    @Override
    public void close() {
        iterators.values().forEach(CloseableIterator::closeIterator);
    }
}
