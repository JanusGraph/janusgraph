package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexArrayList;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexLongList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexListTest {

    @Test
    public void testLists() {

        TitanGraph g = TitanFactory.open("inmemory");
        StandardTitanTx tx = (StandardTitanTx) g.newTransaction();
        VertexLongList vll = new VertexLongList(tx);
        VertexArrayList val = new VertexArrayList(tx);
        for (int i=0; i<10; i++) {
            TitanVertex v = tx.addVertex();
            vll.add(v);
            val.add(v);
        }

        assertEquals(10, Iterables.size(vll));
        assertEquals(10, Iterables.size(val));




        tx.commit();
        g.close();

    }


}
