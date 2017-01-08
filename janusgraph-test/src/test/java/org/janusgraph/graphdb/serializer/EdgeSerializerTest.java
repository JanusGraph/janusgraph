package org.janusgraph.graphdb.serializer;

import org.janusgraph.StorageSetup;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeSerializerTest {


    @Test
    public void testValueOrdering() {
        StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraph();
        JanusGraphManagement mgmt = graph.openManagement();
        EdgeLabel father = mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        for (int i=1;i<=5;i++) mgmt.makePropertyKey("key" + i).dataType(Integer.class).make();
        mgmt.commit();

        JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();
        JanusGraphEdge e1 = v1.addEdge("father",v2);
        for (int i=1;i<=5;i++) e1.property("key"+i,i);

        graph.tx().commit();

        e1.remove();
        graph.tx().commit();

    }


    private Entry serialize(StandardJanusGraph graph, JanusGraphEdge e, int pos) {
        EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        InternalRelation r = (InternalRelation)e;
        return edgeSerializer.writeRelation(r,pos,r.tx());
    }

}
