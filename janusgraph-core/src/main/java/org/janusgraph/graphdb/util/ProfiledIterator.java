// Copyright 2021 JanusGraph Authors
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
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class ProfiledIterator<E extends JanusGraphElement> extends CloseableAbstractIterator<E> {
    private final QueryProfiler profiler;
    private final Iterator<E> iterator;
    private boolean timerRunning;

    public ProfiledIterator(QueryProfiler profiler, Supplier<Iterator<E>> iteratorSupplier) {
        this.profiler = profiler;
        profiler.startTimer();
        timerRunning = true;
        iterator = iteratorSupplier.get();
    }

    @Override
    protected E computeNext() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        close();
        return endOfData();
    }

    @Override
    public void close() {
        if (timerRunning) {
            profiler.stopTimer();
            timerRunning = false;
        }
        CloseableIterator.closeIterator(iterator);
    }
}
