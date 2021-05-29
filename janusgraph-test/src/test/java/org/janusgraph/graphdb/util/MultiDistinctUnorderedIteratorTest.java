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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.janusgraph.graphdb.query.Query;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MultiDistinctUnorderedIteratorTest {
    @Test
    public void shouldConcatEmptyIterators() {
        List<Iterator<Element>> iterators = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            iterators.add(CloseableIteratorUtils.emptyIterator());
        }
        MultiDistinctUnorderedIterator<Element> multiIterator = new MultiDistinctUnorderedIterator<Element>(
            0, Query.NO_LIMIT, iterators);
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }

    @Test
    public void shouldConcatIteratorsAndDedup() {
        List<Iterator<Vertex>> iterators = new ArrayList<>();
        final int num = 5;
        for (int i = 0; i < num; i++) {
            iterators.add(CloseableIteratorUtils.emptyIterator());
            List<Vertex> vertices = Arrays.asList(DetachedVertex.build().setId(i).create(),
                DetachedVertex.build().setId(i + 1).create());
            iterators.add(vertices.iterator());
        }

        MultiDistinctUnorderedIterator<Vertex> multiIterator = new MultiDistinctUnorderedIterator<>(0, Query.NO_LIMIT, iterators);
        for (int i = 0; i <= num; i++) {
            assertEquals(DetachedVertex.build().setId(i).create(), multiIterator.next());
        }
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }

    @Test
    public void shouldSkipLowLimit() {
        List<Iterator<Vertex>> iterators = new ArrayList<>();
        final int num = 5;
        for (int i = 0; i < num; i++) {
            iterators.add(CloseableIteratorUtils.emptyIterator());
            List<Vertex> vertices = Arrays.asList(DetachedVertex.build().setId(i).create(),
                DetachedVertex.build().setId(i + 1).create());
            iterators.add(vertices.iterator());
        }

        MultiDistinctUnorderedIterator<Vertex> multiIterator = new MultiDistinctUnorderedIterator<>(2, Query.NO_LIMIT, iterators);
        for (int i = 2; i <= num; i++) {
            assertEquals(DetachedVertex.build().setId(i).create(), multiIterator.next());
        }
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }

    @Test
    public void shouldStopAtHighLimit() {
        List<Iterator<Vertex>> iterators = new ArrayList<>();
        final int num = 5;
        for (int i = 0; i < num; i++) {
            iterators.add(CloseableIteratorUtils.emptyIterator());
            List<Vertex> vertices = Arrays.asList(DetachedVertex.build().setId(i).create(),
                DetachedVertex.build().setId(i + 1).create());
            iterators.add(vertices.iterator());
        }

        MultiDistinctUnorderedIterator<Vertex> multiIterator = new MultiDistinctUnorderedIterator<>(2, num, iterators);
        for (int i = 2; i < num; i++) {
            assertEquals(DetachedVertex.build().setId(i).create(), multiIterator.next());
        }
        assertFalse(multiIterator.hasNext());
        multiIterator.close();
    }
}
