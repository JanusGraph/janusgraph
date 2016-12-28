package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexArrayList;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexLongList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
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

        TitanGraph g = TitanFactory.open("inmemory");
        StandardTitanTx tx = (StandardTitanTx) g.newTransaction();
        VertexLongList vll = new VertexLongList(tx);
        VertexArrayList val = new VertexArrayList(tx);
        for (int i=0; i<num; i++) {
            TitanVertex v = tx.addVertex();
            vll.add(v);
            val.add(v);
        }

        assertEquals(num, Iterables.size(vll));
        assertEquals(num, Iterables.size(val));

        vll.sort();
        val.sort();
        assertTrue(vll.isSorted());
        assertTrue(val.isSorted());

        for (Iterable<TitanVertex> iterable : new Iterable[]{val,vll}) {
            Iterator<TitanVertex> iter = iterable.iterator();
            TitanVertex previous = null;
            for (int i = 0; i < num; i++) {
                TitanVertex next = iter.next();
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
