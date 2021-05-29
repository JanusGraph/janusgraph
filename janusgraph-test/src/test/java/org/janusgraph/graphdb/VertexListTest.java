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

package org.janusgraph.graphdb;

import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.query.vertex.VertexArrayList;
import org.janusgraph.graphdb.query.vertex.VertexLongList;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexListTest {

    @Test
    public void testLists() {

        int num = 13;

        JanusGraph g = JanusGraphFactory.open("inmemory");
        StandardJanusGraphTx tx = (StandardJanusGraphTx) g.newTransaction();
        VertexLongList vll = new VertexLongList(tx);
        VertexArrayList val = new VertexArrayList(tx);
        for (int i=0; i<num; i++) {
            JanusGraphVertex v = tx.addVertex();
            vll.add(v);
            val.add(v);
        }

        assertEquals(num, Iterables.size(vll));
        assertEquals(num, Iterables.size(val));

        vll.sort();
        val.sort();
        assertTrue(vll.isSorted());
        assertTrue(val.isSorted());

        for (Iterable<JanusGraphVertex> iterable : new Iterable[]{val,vll}) {
            Iterator<JanusGraphVertex> iterator = iterable.iterator();
            JanusGraphVertex previous = null;
            for (int i = 0; i < num; i++) {
                JanusGraphVertex next = iterator.next();
                if (previous!=null) assertTrue(previous.longId()<next.longId());
                previous = next;
            }
            try {
                iterator.next();
                fail();
            } catch (NoSuchElementException ignored) {

            }
        }


        tx.commit();
        g.close();

    }


}
