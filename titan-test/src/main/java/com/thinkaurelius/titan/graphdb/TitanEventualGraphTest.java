package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TypeMaker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanEventualGraphTest extends TitanGraphTestCommon {

    private Logger log = LoggerFactory.getLogger(TitanEventualGraphTest.class);

    public TitanEventualGraphTest(Configuration config) {
        super(config);
    }

    @Test
    public void concurrentIndexTest() {
        TitanKey id = tx.makeType().name("uid").vertexUnique(Direction.OUT).graphUnique().indexed(Vertex.class).dataType(String.class).makePropertyKey();
        TitanKey value = tx.makeType().name("value").vertexUnique(Direction.OUT, TypeMaker.UniquenessConsistency.NO_LOCK).dataType(Object.class).indexed(Vertex.class).makePropertyKey();

        TitanVertex v = tx.addVertex();
        v.setProperty(id, "v");

        clopen();

        //Concurrent index addition
        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        tx1.getVertex(id, "v").setProperty("value", 11);
        tx2.getVertex(id, "v").setProperty("value", 11);
        tx1.commit();
        tx2.commit();

        assertEquals("v", Iterables.getOnlyElement(tx.getVertices("value", 11)).getProperty(id.getName()));

    }


}
