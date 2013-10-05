package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class IndexProviderTest {

    private Logger log = LoggerFactory.getLogger(IndexProviderTest.class);

    private static final Random random = new Random();

    protected IndexProvider index;
    protected IndexTransaction tx;

    public abstract IndexProvider openIndex() throws StorageException;

    @Before
    public void setUp() throws Exception {
        openIndex().clearStorage();
        open();
    }

    public void open() throws StorageException {
        index = openIndex();
        tx = new IndexTransaction(index);
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws StorageException {
        if (tx != null) tx.commit();
        index.close();
    }

    public void clopen() throws StorageException {
        close();
        open();
    }

    @Test
    public void openClose() {

    }

    @Test
    public void singleStore() throws Exception {
        storeTest("vertex");
    }

    @Test
    public void multipleStores() throws Exception {
        storeTest("vertex", "edge");
    }


    private void storeTest(String... stores) throws Exception {

        Map<String, Object> doc1 = getDocument("Hello world", 1001, 5.2, Geoshape.point(48.0, 0.0));
        Map<String, Object> doc2 = getDocument("Tomorrow is the world", 1010, 8.5, Geoshape.point(49.0, 1.0));
        Map<String, Object> doc3 = getDocument("Hello Bob, are you there?", -500, 10.1, Geoshape.point(47.0, 10.0));

        for (String store : stores) {
            initialize(store);

            add(store, "doc1", doc1, true);
            add(store, "doc2", doc2, true);
            add(store, "doc3", doc3, false);

        }

        ImmutableList<IndexQuery.OrderEntry> orderTimeAsc = ImmutableList.of(new IndexQuery.OrderEntry("time", Order.ASC, Integer.class));
        ImmutableList<IndexQuery.OrderEntry> orderWeightAsc = ImmutableList.of(new IndexQuery.OrderEntry("weight", Order.ASC, Double.class));
        ImmutableList<IndexQuery.OrderEntry> orderTimeDesc = ImmutableList.of(new IndexQuery.OrderEntry("time", Order.DESC, Integer.class));
        ImmutableList<IndexQuery.OrderEntry> orderWeightDesc = ImmutableList.of(new IndexQuery.OrderEntry("weight", Order.DESC, Double.class));
        ImmutableList<IndexQuery.OrderEntry> jointOrder = ImmutableList.of(new IndexQuery.OrderEntry("weight", Order.DESC, Double.class), new IndexQuery.OrderEntry("time", Order.DESC, Integer.class));


        clopen();

        for (String store : stores) {

            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "wOrLD")))));
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "worl"))).size());

            //Ordering
            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world"), orderTimeDesc));
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world"), orderWeightDesc));
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world"), orderTimeAsc));
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world"), orderWeightAsc));
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world"), jointOrder));
            assertEquals(ImmutableList.of("doc2", "doc1"), result);

            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.PREFIX, "w")));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.PREFIX, "wOr")));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));

            if (index.supports(String.class, Text.REGEX)) {
                result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.REGEX, "he[l]+(.*)")));
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.REGEX, "[h]+e[l]+(.*)")));
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.REGEX, "he[l]+")));
                assertTrue(result.isEmpty());
                result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.REGEX, "e[l]+(.*)")));
                assertTrue(result.isEmpty());
            }

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "world"), PredicateCondition.of("text", Text.CONTAINS, "hello"))));
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "Bob")));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "Bob"))));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "bob")));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "world"), PredicateCondition.of("weight", Cmp.GREATER_THAN, 6.0))));
            assertEquals(1, result.size());
            assertEquals("doc2", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00))));
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "tomorrow"), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)))));
            assertEquals(ImmutableSet.of("doc2"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("time", Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of("time", Cmp.LESS_THAN, 1010), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("weight", Cmp.GREATER_THAN, 10.0))));
            assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0))));
            assertEquals(0, result.size());

            //Update some data
            add(store, "doc4", getDocument("I'ts all a big Bob", -100, 11.2, Geoshape.point(48.0, 8.0)), true);
            remove(store, "doc2", doc2, true);
            remove(store, "doc3", ImmutableMap.of("weight", (Object) 10.1), false);
            add(store, "doc3", ImmutableMap.of("time", (Object) 2000, "text", "Bob owns the world"), false);
            remove(store, "doc1", ImmutableMap.of("time", (Object) 1001), false);
            add(store, "doc1", ImmutableMap.of("time", (Object) 1005, "weight", 11.1), false);


        }

        clopen();

        for (String store : stores) {

            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of("text", Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "world"), PredicateCondition.of("weight", Cmp.GREATER_THAN, 6.0))));
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00))));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "tomorrow"), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)))));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("time", Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of("time", Cmp.LESS_THAN, 1010), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
            assertEquals(ImmutableSet.of("doc1", "doc4"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("weight", Cmp.GREATER_THAN, 10.0))));
            assertEquals(ImmutableSet.of("doc1", "doc4"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0))));
            assertEquals(0, result.size());

        }

    }


    @Test
    public void largeTest() throws Exception {
        int numDoc = 30000;
        String store = "vertex";
        initialize(store);
        for (int i = 1; i <= numDoc; i++) {
            add(store, "doc" + i, getRandomDocument(), true);
        }
        clopen();

//        List<String> result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("weight", Cmp.INTERVAL, Interval.of(0.2,0.3)))));
//        List<String> result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("location", Geo.WITHIN,Geoshape.circle(48.5,0.5,1000.00)))));
        long time = System.currentTimeMillis();
        List<String> result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("weight", Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of("weight", Cmp.LESS_THAN, 0.6), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
        int oldresultSize = result.size();
        System.out.println(result.size() + " vs " + (numDoc / 1000 * 2.4622623015));
        System.out.println("Query time on " + numDoc + " docs (ms): " + (System.currentTimeMillis() - time));
        result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("weight", Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of("weight", Cmp.LESS_THAN, 0.6), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))), numDoc / 1000));
        assertEquals(numDoc / 1000, result.size());
        result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("weight", Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of("weight", Cmp.LESS_THAN, 0.6), PredicateCondition.of("location", Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))), numDoc / 1000 * 100));
        assertEquals(oldresultSize, result.size());
    }

    private void initialize(String store) throws StorageException {
        if (index.supports(String.class)) index.register(store, "text", String.class, tx);
        if (index.supports(Long.class)) index.register(store, "time", Long.class, tx);
        if (index.supports(Double.class)) index.register(store, "weight", Double.class, tx);
        if (index.supports(Geoshape.class)) index.register(store, "location", Geoshape.class, tx);
    }

    private void add(String store, String docid, Map<String, Object> doc, boolean isNew) {
        for (Map.Entry<String, Object> kv : doc.entrySet()) {
            if (index.supports(kv.getValue().getClass())) {
                tx.add(store, docid, kv.getKey(), kv.getValue(), isNew);
            }
        }
    }

    private void remove(String store, String docid, Map<String, Object> doc, boolean deleteAll) {
        for (Map.Entry<String, Object> kv : doc.entrySet()) {
            if (index.supports(kv.getValue().getClass())) {
                tx.delete(store, docid, kv.getKey(), deleteAll);
            }
        }
    }


    public static Map<String, Object> getDocument(final String txt, final long time, final double weight, final Geoshape geo) {
        return new HashMap<String, Object>() {{
            put("text", txt);
            put("time", time);
            put("weight", weight);
            put("location", geo);
        }};
    }

    public static Map<String, Object> getRandomDocument() {
        final StringBuilder s = new StringBuilder();
        for (int i = 0; i < 3; i++) s.append(RandomGenerator.randomString(5, 8)).append(" ");
        return new HashMap<String, Object>() {{
            put("text", s.toString());
            put("time", Math.abs(random.nextLong()));
            put("weight", random.nextDouble());
            put("location", Geoshape.point(random.nextDouble() * 180 - 90, random.nextDouble() * 360 - 180));
        }};
    }

}
