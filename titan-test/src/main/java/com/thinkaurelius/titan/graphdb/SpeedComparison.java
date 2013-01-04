package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.TitanQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SpeedComparison extends TitanGraphTestCommon {

    private static final int numVertices = 2000;
    private static final int edgesPerVertex = 400;

    public SpeedComparison() {
        super(getConfig());
    }

    private static final Configuration getConfig() {
        //TODO: Configuration config = StorageSetup.getBerkeleyJEGraphConfiguration();
        //config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY,false);
        return new Configuration() {
            @Override
            public Configuration subset(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isEmpty() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean containsKey(String s) {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void addProperty(String s, Object o) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void setProperty(String s, Object o) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void clearProperty(String s) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void clear() {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Object getProperty(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Iterator getKeys(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Iterator getKeys() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Properties getProperties(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean getBoolean(String s) {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean getBoolean(String s, boolean b) {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Boolean getBoolean(String s, Boolean aBoolean) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public byte getByte(String s) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public byte getByte(String s, byte b) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Byte getByte(String s, Byte aByte) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public double getDouble(String s) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public double getDouble(String s, double v) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Double getDouble(String s, Double aDouble) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public float getFloat(String s) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public float getFloat(String s, float v) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Float getFloat(String s, Float aFloat) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public int getInt(String s) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public int getInt(String s, int i) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Integer getInteger(String s, Integer integer) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public long getLong(String s) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public long getLong(String s, long l) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Long getLong(String s, Long aLong) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public short getShort(String s) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public short getShort(String s, short i) {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Short getShort(String s, Short aShort) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public BigDecimal getBigDecimal(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public BigDecimal getBigDecimal(String s, BigDecimal bigDecimal) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public BigInteger getBigInteger(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public BigInteger getBigInteger(String s, BigInteger bigInteger) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public String getString(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public String getString(String s, String s2) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public String[] getStringArray(String s) {
                return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public List getList(String s) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public List getList(String s, List list) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        graphdb.createKeyIndex("uid", Vertex.class);
        Vertex vertices[] = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            vertices[i] = graphdb.addVertex(null);
            vertices[i].setProperty("uid", i);
        }
        for (int i = 0; i < numVertices; i++) {
            for (int j = 1; j <= edgesPerVertex; j++) {
                graphdb.addEdge(null, vertices[i], vertices[wrapAround(i + j, numVertices)], "connect");
            }
        }
        graphdb.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    }

    @Test
    public void compare() {
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                System.out.print("In Memory - ");
                retrieveNgh(true);
            } else {
                System.out.print("Direct - ");
                retrieveNgh(false);
            }
        }

    }


    public void retrieveNgh(boolean inMemory) {
        long time = time();
        Vertex vertices[] = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) vertices[i] = graphdb.getVertices("uid", i).iterator().next();
        time = time() - time;
        //System.out.println("Vertex retrieval: " + time);

        for (int t = 0; t < 4; t++) {
            time = time();
            for (int i = 0; i < numVertices; i++) {
                TitanQuery q = ((TitanQuery) vertices[i].query()).direction(Direction.OUT).labels("connect");
                if (inMemory) {
                    for (Vertex v : q.inMemory().vertices()) {
                        v.getId();
                    }
                } else {
                    VertexList vl = q.vertexIds();
                    for (int j = 0; j < vl.size(); j++) {
                        vl.get(j);
                    }
                }
            }
            time = time() - time;
            System.out.println("Ngh retrieval: " + time);
        }

        graphdb.stopTransaction(TransactionalGraph.Conclusion.FAILURE);
    }

    private static long time() {
        return System.currentTimeMillis();
    }

}
