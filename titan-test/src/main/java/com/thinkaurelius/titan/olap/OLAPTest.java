package com.thinkaurelius.titan.olap;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.olap.OLAPJob;
import com.thinkaurelius.titan.core.olap.OLAPJobBuilder;
import com.thinkaurelius.titan.core.olap.StateInitializer;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends TitanGraphBaseTest {


    protected abstract <S> OLAPJobBuilder<S> getOLAPBuilder(StandardTitanGraph graph, Class<S> clazz);

    @Test
    public void degreeCount() throws Exception {
        mgmt.makeKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        finishSchema();
        int numV = 100;
        TitanVertex[] vs = new TitanVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex();
            vs[i].setProperty("uid",i+1);
        }
        Random random = new Random();
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            TitanVertex v = vs[i];
            for (int j=0;j<edges;j++) {
                TitanVertex u = vs[random.nextInt(numV)];
                v.addEdge("knows", u);
            }
        }
        clopen();

        OLAPJobBuilder<Degree> builder = getOLAPBuilder(graph,Degree.class);
        builder.setInitializer(new StateInitializer<Degree>() {
            @Override
            public Degree initialState() {
                return new Degree();
            }
        });
        builder.setNumProcessingThreads(1);
        builder.setStateKey("degree");
        builder.setJob(new OLAPJob() {
            @Override
            public void process(TitanVertex vertex) {
                Degree d = vertex.getProperty("degree");
                assertNotNull(d);
                d.in+=vertex.query().direction(Direction.IN).count();
                d.out+= Iterables.size(vertex.getEdges(Direction.OUT));
                d.both+= vertex.query().count();
            }
        });
        builder.addQuery().edges();
        Map<Long,Degree> degrees = builder.execute().get();//10, TimeUnit.SECONDS);
        assertNotNull(degrees);
        assertEquals(numV,degrees.size());
        int totalCount = 0;
        for (Map.Entry<Long,Degree> entry : degrees.entrySet()) {
            Degree degree = entry.getValue();
            assertEquals(degree.in+degree.out,degree.both);
            Vertex v = tx.getVertex(entry.getKey().longValue());
            int count = v.getProperty("uid");
            assertEquals(count,degree.out);
            totalCount+= degree.both;
        }
        assertEquals(numV*(numV+1),totalCount);
    }

    public class Degree {
        public int in;
        public int out;
        public int both;

        public Degree() {
            in=0;
            out=0;
            both=0;
        }
    }


}
