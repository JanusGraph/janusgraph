package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.Entry;
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
        TitanManagement mgmt = graph.openManagement();
        EdgeLabel father = mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        for (int i=1;i<=5;i++) mgmt.makePropertyKey("key" + i).dataType(Integer.class).make();
        mgmt.commit();

        TitanVertex v1 = graph.addVertex(), v2 = graph.addVertex();
        TitanEdge e1 = v1.addEdge("father",v2);
        for (int i=1;i<=5;i++) e1.property("key"+i,i);

        graph.tx().commit();

        e1.remove();
        graph.tx().commit();

    }


    private Entry serialize(StandardTitanGraph graph, TitanEdge e, int pos) {
        EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        InternalRelation r = (InternalRelation)e;
        return edgeSerializer.writeRelation(r,pos,r.tx());
    }

}
