// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.query.vertex;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.CacheVertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class VertexIdListTest {

    @Mock
    private StandardJanusGraphTx tx;

    @Test
    public void testMergeSortedLongIdLists() {
        VertexIdList list = new VertexIdList(tx, Arrays.asList(1L, 2L, 3L), true);
        VertexIdList otherList = new VertexIdList(tx, Arrays.asList(0L, 2L, 4L), true);
        list.addAll(otherList);
        assertEquals(Arrays.asList(0L, 1L, 2L, 2L, 3L, 4L), list.getIDs());
        assertTrue(list.isSorted());
    }

    @Test
    public void testMergeUnsortedLongIdLists() {
        VertexIdList list = new VertexIdList(tx, new ArrayList<>(Arrays.asList(4L, 2L, 3L)), false);
        VertexIdList otherList = new VertexIdList(tx, Arrays.asList(0L, 5L, 4L), false);
        list.addAll(otherList);
        assertEquals(Arrays.asList(4L, 2L, 3L, 0L, 5L, 4L), list.getIDs());
        assertFalse(list.isSorted());
    }

    @Test
    public void testMergeSortedStringIdLists() {
        VertexIdList list = new VertexIdList(tx, Arrays.asList("x1", "x2", "y1"), true);
        VertexIdList otherList = new VertexIdList(tx, Arrays.asList("x2", "x4"), true);
        list.addAll(otherList);
        assertEquals(Arrays.asList("x1", "x2", "x2", "x4", "y1"), list.getIDs());
        assertTrue(list.isSorted());
    }

    @Test
    public void testMergeSortedHeterogeneousIdLists() {
        VertexIdList list = new VertexIdList(tx, Arrays.asList("x1", "x2", "y1"), true);
        VertexIdList otherList = new VertexIdList(tx, Arrays.asList(2, 4), true);
        list.addAll(otherList);
        assertEquals(Arrays.asList(2, 4, "x1", "x2", "y1"), list.getIDs());
        assertTrue(list.isSorted());
    }

    @Test
    public void testMergeSortedWithUnsortedIdLists() {
        VertexIdList list = new VertexIdList(tx, new ArrayList<>(Arrays.asList("x1", "x2", "y1")), true);
        VertexIdList otherList = new VertexIdList(tx, Arrays.asList(2, 4), false);
        list.addAll(otherList);
        assertEquals(Arrays.asList("x1", "x2", "y1", 2, 4), list.getIDs());
        assertFalse(list.isSorted());
    }

    @Test
    public void testMergeUnsortedHeterogeneousIdLists() {
        VertexIdList list = new VertexIdList(tx, new ArrayList<>(Arrays.asList("x1", "x2", "y1")), false);
        VertexIdList otherList = new VertexIdList(tx, Arrays.asList("x3", 4, 2), false);
        list.addAll(otherList);
        assertEquals(Arrays.asList("x1", "x2", "y1", "x3", 4, 2), list.getIDs());
        assertFalse(list.isSorted());
    }

    @Test
    public void testMergeSortedVertexLists() {
        VertexIdList list = new VertexIdList(tx, Arrays.asList("x1", "x2", "y1"), true);
        VertexArrayList otherList = new VertexArrayList(tx);
        otherList.add(new CacheVertex(tx, "x2", ElementLifeCycle.Loaded));
        otherList.add(new CacheVertex(tx, "x4", ElementLifeCycle.Loaded));
        list.addAll(otherList);
        assertEquals(Arrays.asList("x1", "x2", "x2", "x4", "y1"), list.getIDs());
        assertTrue(list.isSorted());
    }

    @Test
    public void testIterator() {
        VertexIdList list = new VertexIdList(tx, Arrays.asList("x1", "x2"), true);
        Iterator<JanusGraphVertex> iter = list.iterator();
        Vertex v1 = new CacheVertex(tx, "x1", ElementLifeCycle.Loaded);
        when(tx.getInternalVertex("x1")).thenReturn((InternalVertex) v1);
        assertEquals(v1, iter.next());
        Vertex v2 = new CacheVertex(tx, "x2", ElementLifeCycle.Loaded);
        when(tx.getInternalVertex("x2")).thenReturn((InternalVertex) v2);
        assertEquals(v2, iter.next());
    }
}
