package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.Mapping;
import com.thinkaurelius.titan.testutil.TestUtil;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class TitanIndexTest extends TitanGraphTestCommon {

    public static final String INDEX = "index";

    public final boolean supportsGeoPoint;
    public final boolean supportsNumeric;
    public final boolean supportsText;

    protected TitanIndexTest(Configuration config, boolean supportsGeoPoint, boolean supportsNumeric, boolean supportsText) {
        super(config);
        this.supportsGeoPoint = supportsGeoPoint;
        this.supportsNumeric = supportsNumeric;
        this.supportsText = supportsText;
    }

    public abstract boolean supportsLuceneStyleQueries();

    @Test
    public void testOpenClose() {
    }

    @Test
    public void testSimpleUpdate() {
        TitanKey text = tx.makeKey("name").single()
                .indexed(INDEX, Vertex.class).indexed(INDEX, Edge.class).dataType(String.class).make();
        Vertex v = tx.addVertex();
        v.setProperty("name", "Marko Rodriguez");
        assertEquals(1, Iterables.size(tx.query().has("name", Text.CONTAINS, "marko").vertices()));
        clopen();
        Iterable<Vertex> vs = tx.query().has("name", Text.CONTAINS, "marko").vertices();
        assertEquals(1, Iterables.size(vs));
        v = vs.iterator().next();
        v.setProperty("name", "Marko");
        clopen();
        vs = tx.query().has("name", Text.CONTAINS, "marko").vertices();
        assertEquals(1, Iterables.size(vs));
        v = vs.iterator().next();

    }

    @Test
    public void testIndexing() {
        TitanKey text = tx.makeKey("text").single()
                .indexed(INDEX, Vertex.class).indexed(INDEX, Edge.class).dataType(String.class).make();
        TitanKey location = tx.makeKey("location").single()
                .indexed(INDEX, Vertex.class).indexed(INDEX, Edge.class).dataType(Geoshape.class).make();
        TitanKey time = tx.makeKey("time").single()
                .indexed(INDEX, Vertex.class).indexed(INDEX, Edge.class).dataType(Long.class).make();
        TitanKey category = tx.makeKey("category").single()
                .indexed(Vertex.class).indexed(Edge.class).dataType(Integer.class).make();
        TitanKey group = tx.makeKey("group").single()
                .indexed(INDEX, Vertex.class).indexed(INDEX, Edge.class).dataType(Byte.class).make();
        TitanKey id = tx.makeKey("uid").single().unique()
                .indexed(Vertex.class).dataType(Integer.class).make();
        TitanLabel knows = tx.makeLabel("knows").sortKey(time).signature(location).make();

        clopen();
        String[] words = {"world", "aurelius", "titan", "graph"};
        int numCategories = 5;
        int numGroups = 10;
        double distance, offset;
        int numV = 100;
        final int originalNumV = numV;
        for (int i = 0; i < numV; i++) {
            Vertex v = tx.addVertex();
            v.setProperty("uid", i);
            v.setProperty("category", i % numCategories);
            v.setProperty("group", i % numGroups);
            v.setProperty("text", "Vertex " + words[i % words.length]);
            v.setProperty("time", i);
            offset = (i % 2 == 0 ? 1 : -1) * (i * 50.0 / numV);
            v.setProperty("location", Geoshape.point(0.0 + offset, 0.0 + offset));

            Edge e = v.addEdge("knows", tx.getVertex("uid", Math.max(0, i - 1)));
            e.setProperty("text", "Vertex " + words[i % words.length]);
            e.setProperty("time", i);
            e.setProperty("category", i % numCategories);
            e.setProperty("group", i % numGroups);
            e.setProperty("location", Geoshape.point(0.0 + offset, 0.0 + offset));
        }

        for (int i = 0; i < words.length; i++) {
            int expectedSize = numV / words.length;
            assertEquals(expectedSize, Iterables.size(tx.query().has("text", Text.CONTAINS, words[i]).vertices()));
            assertEquals(expectedSize, Iterables.size(tx.query().has("text", Text.CONTAINS, words[i]).edges()));

            //Test ordering
            for (String orderKey : new String[]{"time", "category"}) {
                for (Order order : Order.values()) {
                    for (Iterable<? extends Element> iter : ImmutableList.of(
                            tx.query().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order).vertices(),
                            tx.query().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order).edges()
                    )) {
                        Element previous = null;
                        int count = 0;
                        for (Element element : iter) {
                            if (previous != null) {
                                int cmp = ((Comparable) element.getProperty(orderKey)).compareTo(previous.getProperty(orderKey));
                                assertTrue(element.getProperty(orderKey) + " <> " + previous.getProperty(orderKey),
                                        order == Order.ASC ? cmp >= 0 : cmp <= 0);
                            }
                            previous = element;
                            count++;
                        }
                        assertEquals(expectedSize, count);
                    }
                }
            }
        }

        assertEquals(3, Iterables.size(tx.query().has("group", 3).orderBy("time", Order.ASC).limit(3).vertices()));
        assertEquals(3, Iterables.size(tx.query().has("group", 3).orderBy("time", Order.DESC).limit(3).edges()));

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertEquals(i, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).vertices()));
            assertEquals(i, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).edges()));
        }

        for (int i = 0; i < numV; i += 10) {
            offset = (i * 50.0 / originalNumV);
            distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
            assertEquals(i + 1, Iterables.size(tx.query().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).vertices()));
            assertEquals(i + 1, Iterables.size(tx.query().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).edges()));
        }

        //Mixed index queries
        assertEquals(4, Iterables.size(tx.query().has("category", 1).interval("time", 10, 28).vertices()));
        assertEquals(4, Iterables.size(tx.query().has("category", 1).interval("time", 10, 28).edges()));

        assertEquals(5, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]).vertices()));
        offset = (19 * 50.0 / originalNumV);
        distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertEquals(5, Iterables.size(tx.query().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]).vertices()));

        assertEquals(numV, Iterables.size(tx.getVertices()));
        assertEquals(numV, Iterables.size(tx.getEdges()));

        clopen();

        //##########################
        //Copied from above
        //##########################

        for (int i = 0; i < words.length; i++) {
            int expectedSize = numV / words.length;
            assertEquals(expectedSize, Iterables.size(tx.query().has("text", Text.CONTAINS, words[i]).vertices()));
            assertEquals(expectedSize, Iterables.size(tx.query().has("text", Text.CONTAINS, words[i]).edges()));

            //Test ordering
            for (String orderKey : new String[]{"time", "category"}) {
                for (Order order : Order.values()) {
                    for (Iterable<? extends Element> iter : ImmutableList.of(
                            tx.query().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order).vertices(),
                            tx.query().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order).edges()
                    )) {
                        TestUtil.verifyElementOrder(iter,orderKey,order,expectedSize);
                    }
                }
            }
        }

        assertEquals(3, Iterables.size(tx.query().has("group", 3).orderBy("time", Order.ASC).limit(3).vertices()));
        assertEquals(3, Iterables.size(tx.query().has("group", 3).orderBy("time", Order.DESC).limit(3).edges()));

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertEquals(i, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).vertices()));
            assertEquals(i, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).edges()));
        }

        for (int i = 0; i < numV; i += 10) {
            offset = (i * 50.0 / originalNumV);
            distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
            assertEquals(i + 1, Iterables.size(tx.query().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).vertices()));
            assertEquals(i + 1, Iterables.size(tx.query().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).edges()));
        }

        //Mixed index queries
        assertEquals(4, Iterables.size(tx.query().has("category", 1).interval("time", 10, 28).vertices()));
        assertEquals(4, Iterables.size(tx.query().has("category", 1).interval("time", 10, 28).edges()));

        assertEquals(5, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]).vertices()));
        offset = (19 * 50.0 / originalNumV);
        distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertEquals(5, Iterables.size(tx.query().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]).vertices()));

        assertEquals(numV, Iterables.size(tx.getVertices()));
        assertEquals(numV, Iterables.size(tx.getEdges()));

        newTx();

        int numDelete = 12;
        for (int i = numV - numDelete; i < numV; i++) {
            tx.getVertex("uid", i).remove();
        }

        numV = numV - numDelete;

        //Copied from above
        for (int i = 0; i < words.length; i++) {
            assertEquals(numV / words.length, Iterables.size(tx.query().has("text", Text.CONTAINS, words[i]).vertices()));
            assertEquals(numV / words.length, Iterables.size(tx.query().has("text", Text.CONTAINS, words[i]).edges()));
        }

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertEquals(i, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).vertices()));
            assertEquals(i, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).edges()));
        }

        for (int i = 0; i < numV; i += 10) {
            offset = (i * 50.0 / originalNumV);
            distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
            assertEquals(i + 1, Iterables.size(tx.query().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).vertices()));
            assertEquals(i + 1, Iterables.size(tx.query().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).edges()));
        }

        assertEquals(5, Iterables.size(tx.query().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]).vertices()));
        offset = (19 * 50.0 / originalNumV);
        distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertEquals(5, Iterables.size(tx.query().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]).vertices()));

        assertEquals(numV, Iterables.size(tx.getVertices()));
        assertEquals(numV, Iterables.size(tx.getEdges()));

    }

    private void setupChainGraph(int numV, String[] strs) {
        TitanKey name = graph.makeKey("name").dataType(String.class).indexed(INDEX, Element.class, Parameter.of(Mapping.MAPPING_PREFIX,Mapping.STRING)).single().make();
        TitanKey text = graph.makeKey("text").dataType(String.class).indexed(INDEX, Element.class, Parameter.of(Mapping.MAPPING_PREFIX,Mapping.TEXT)).single().make();
        graph.makeLabel("knows").sortKey(name).sortOrder(Order.DESC).make();
        TitanVertex previous = null;
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty("name",strs[i%strs.length]);
            v.setProperty("text",strs[i%strs.length]);
            TitanEdge e = v.addEdge("knows",previous==null?v:previous);
            e.setProperty("name",strs[i%strs.length]);
            e.setProperty("text",strs[i%strs.length]);
            previous=v;
        }
    }

    @Test
    public void testIndexParameters() {
        int numV = 1000;
        String[] strs = {"Uncle Berry has a farm","and on his farm he has five ducks","ducks are beautiful animals","the sky is very blue today"};
        setupChainGraph(numV,strs);

        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").vertices()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).vertices()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").vertices()));

        //Same queries for edges
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").edges()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).edges()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").edges()));


        clopen();
        //Same queries as above but against backend

        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").vertices()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).vertices()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").vertices()));

        //Same queries for edges
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").edges()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).edges()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").edges()));

    }

    @Test
    public void testRawQueries() {
        if (!supportsLuceneStyleQueries()) return;

        int numV = 1000;
        String[] strs = {"Uncle Berry has a farm","and on his farm he has five ducks","ducks are beautiful animals","the sky is very blue today"};
        setupChainGraph(numV,strs);
        clopen();

        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(INDEX,"v.text:ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(INDEX,"v.text:(farm uncle berry)").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.indexQuery(INDEX,"v.text:(farm uncle berry) AND v.name:\"Uncle Berry has a farm\"").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(INDEX,"v.text:(beautiful are ducks)").vertices()));
        assertEquals(10,Iterables.size(graph.indexQuery(INDEX,"v.\"text\":(beautiful are ducks)").limit(10).vertices()));

        //Same queries for edges
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(INDEX,"e.text:ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(INDEX,"e.text:(farm uncle berry)").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.indexQuery(INDEX,"e.text:(farm uncle berry) AND e.name:\"Uncle Berry has a farm\"").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(INDEX,"e.text:(beautiful are ducks)").edges()));
        assertEquals(10,Iterables.size(graph.indexQuery(INDEX,"e.\"text\":(beautiful are ducks)").limit(10).edges()));

    }

    @Test
    public void testIndexIteration() {
        graph.makeKey("objectType").dataType(String.class).indexed(INDEX, Vertex.class).single().make();
        graph.makeKey("uid").dataType(Long.class).indexed(INDEX, Vertex.class).single().make();
        graph.commit();
        Vertex v = graph.addVertex(null);
        ElementHelper.setProperties(v, "uid", 167774517, "ipv4Addr", "10.0.9.53", "cid", 2, "objectType", "NetworkSensor", "observationDomain", 0);
        assertNotNull(v.getProperty("uid"));
        assertNotNull(v.getProperty("objectType"));
        graph.commit();
        assertNotNull(graph.getVertex(v).getProperty("uid"));
        assertNotNull(graph.getVertex(v).getProperty("objectType"));
        graph.commit();
        for (Vertex u : graph.getVertices()) {
            assertNotNull(u.getProperty("uid"));
            assertNotNull(u.getProperty("objectType"));
        }
        graph.rollback();

    }

}
