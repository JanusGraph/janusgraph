package org.janusgraph.graphdb;

import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusVertex;
import org.janusgraph.graphdb.query.vertex.VertexArrayList;
import org.janusgraph.graphdb.query.vertex.VertexLongList;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexListTest {

    @Test
    public void testLists() {

        int num = 13;

        JanusGraph g = JanusFactory.open("inmemory");
        StandardJanusTx tx = (StandardJanusTx) g.newTransaction();
        VertexLongList vll = new VertexLongList(tx);
        VertexArrayList val = new VertexArrayList(tx);
        for (int i=0; i<num; i++) {
            JanusVertex v = tx.addVertex();
            vll.add(v);
            val.add(v);
        }

        assertEquals(num, Iterables.size(vll));
        assertEquals(num, Iterables.size(val));

        vll.sort();
        val.sort();
        assertTrue(vll.isSorted());
        assertTrue(val.isSorted());

        for (Iterable<JanusVertex> iterable : new Iterable[]{val,vll}) {
            Iterator<JanusVertex> iter = iterable.iterator();
            JanusVertex previous = null;
            for (int i = 0; i < num; i++) {
                JanusVertex next = iter.next();
                if (previous!=null) assertTrue(previous.longId()<next.longId());
                previous = next;
            }
            try {
                iter.next();
                fail();
            } catch (NoSuchElementException ex) {

            }
        }


        tx.commit();
        g.close();

    }


}
