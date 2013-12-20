package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeSerializerTest {


    @Test
    public void testValueOrdering() {
        StandardTitanGraph graph = (StandardTitanGraph) StorageSetup.getInMemoryGraph();
        TitanLabel father = graph.makeLabel("father").manyToOne().make();
        for (int i=1;i<=5;i++) graph.makeKey("key"+i).single().dataType(Integer.class).make();

        TitanVertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);
        TitanEdge e1 = v1.addEdge("father",v2);
        for (int i=1;i<=5;i++) e1.setProperty("key"+i,i);

        graph.commit();

        e1.remove();
        graph.commit();

    }


    private Entry serialize(StandardTitanGraph graph, TitanEdge e, int pos) {
        EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        InternalRelation r = (InternalRelation)e;
        return edgeSerializer.writeRelation(r,pos,r.tx());
    }

}
