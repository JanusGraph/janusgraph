package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Mapping;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
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
import static org.junit.Assert.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class IndexProviderTest {

    private Logger log = LoggerFactory.getLogger(IndexProviderTest.class);

    private static final Random random = new Random();
    private static final Parameter[] NO_PARAS = new Parameter[0];

    protected IndexProvider index;
    protected IndexTransaction tx;

    public static final String TEXT = "text", TIME = "time", WEIGHT = "weight", LOCATION = "location", NAME = "name";

    public static final Map<String,KeyInformation> allKeys = new HashMap<String,KeyInformation>() {{
        put(TEXT,new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.TEXT)));
        put(TIME,new StandardKeyInformation(Long.class));
        put(WEIGHT,new StandardKeyInformation(Double.class, new Parameter("mapping",Mapping.DEFAULT)));
        put(LOCATION,new StandardKeyInformation(Geoshape.class));
        put(NAME,new StandardKeyInformation(String.class, new Parameter("mapping",Mapping.STRING)));
    }};

    public static final KeyInformation.IndexRetriever indexRetriever = new KeyInformation.IndexRetriever() {

        @Override
        public KeyInformation get(String store, String key) {
            //Same for all stores
            return allKeys.get(key);
        }

        @Override
        public KeyInformation.StoreRetriever get(String store) {
            return new KeyInformation.StoreRetriever() {
                @Override
                public KeyInformation get(String key) {
                    return allKeys.get(key);
                }
            };
        }
    };

    public static StandardKeyInformation of(Class<?> clazz, Parameter... paras) {
        return new StandardKeyInformation(clazz,paras);
    }

    public abstract IndexProvider openIndex() throws StorageException;

    public abstract boolean supportsLuceneStyleQueries();

    @Before
    public void setUp() throws Exception {
        openIndex().clearStorage();
        open();
    }

    public void open() throws StorageException {
        index = openIndex();
        tx = new IndexTransaction(index, indexRetriever);
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

        ImmutableList<IndexQuery.OrderEntry> orderTimeAsc = ImmutableList.of(new IndexQuery.OrderEntry(TIME, Order.ASC, Integer.class));
        ImmutableList<IndexQuery.OrderEntry> orderWeightAsc = ImmutableList.of(new IndexQuery.OrderEntry(WEIGHT, Order.ASC, Double.class));
        ImmutableList<IndexQuery.OrderEntry> orderTimeDesc = ImmutableList.of(new IndexQuery.OrderEntry(TIME, Order.DESC, Integer.class));
        ImmutableList<IndexQuery.OrderEntry> orderWeightDesc = ImmutableList.of(new IndexQuery.OrderEntry(WEIGHT, Order.DESC, Double.class));
        ImmutableList<IndexQuery.OrderEntry> jointOrder = ImmutableList.of(new IndexQuery.OrderEntry(WEIGHT, Order.DESC, Double.class), new IndexQuery.OrderEntry(TIME, Order.DESC, Integer.class));


        clopen();

        for (String store : stores) {
            //Token
            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "wOrLD")))));
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "worl"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "Tomorrow is the world"))).size());

            //Ordering
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderTimeDesc));
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderWeightDesc));
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderTimeAsc));
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderWeightAsc));
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), jointOrder));
            assertEquals(ImmutableList.of("doc2", "doc1"), result);

            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_PREFIX, "w")));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_PREFIX, "wOr")));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            assertEquals(0,tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_PREFIX, "bobi"))).size());

            if (index.supports(new StandardKeyInformation(String.class), Text.CONTAINS_REGEX)) {
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "he[l]+(.*)")));
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "[h]+e[l]+(.*)")));
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "he[l]+")));
                assertTrue(result.isEmpty());
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "e[l]+(.*)")));
                assertTrue(result.isEmpty());
            }
            for (TitanPredicate tp : new Text[]{Text.PREFIX, Text.REGEX}) {
                try {
                    assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, tp, "world"))).size());
                    fail();
                } catch (IllegalArgumentException e) {}
            }

            //String
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "Tomorrow is the world"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "world"))).size());
            assertEquals(3, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.NOT_EQUAL, "bob"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.PREFIX, "Tomorrow"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.PREFIX, "wor"))).size());
            for (TitanPredicate tp : new Text[]{Text.CONTAINS,Text.CONTAINS_PREFIX, Text.CONTAINS_REGEX}) {
                try {
                    assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, tp, "world"))).size());
                    fail();
                } catch (IllegalArgumentException e) {}
            }
            if (index.supports(new StandardKeyInformation(String.class), Text.REGEX)) {
                assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomo[r]+ow is.*world"))).size());
                assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomorrow"))).size());
            }

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(TEXT, Text.CONTAINS, "hello"))));
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "Bob")));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "Bob"))));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "bob")));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 6.0))));
            assertEquals(1, result.size());
            assertEquals("doc2", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00))));
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "tomorrow"), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)))));
            assertEquals(ImmutableSet.of("doc2"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of(TIME, Cmp.LESS_THAN, 1010), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 10.0))));
            assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0))));
            assertEquals(0, result.size());

            if (supportsLuceneStyleQueries()) {
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:\"Hello Bob\"",NO_PARAS))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:(world AND tomorrow)",NO_PARAS))));
//                printResult(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS)));
                assertEquals(2, Iterables.size(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS))));
                assertEquals(2, Iterables.size(tx.query(new RawQuery(store,"text:\"world\"",NO_PARAS))));
                assertEquals(2, Iterables.size(tx.query(new RawQuery(store,"time:[1000 TO 1020]",NO_PARAS))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:world AND time:1001",NO_PARAS))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"name:\"Hello world\"",NO_PARAS))));
            }

            //Update some data
            add(store, "doc4", getDocument("I'ts all a big Bob", -100, 11.2, Geoshape.point(48.0, 8.0)), true);
            remove(store, "doc2", doc2, true);
            remove(store, "doc3", ImmutableMap.of(WEIGHT, (Object) 10.1), false);
            add(store, "doc3", ImmutableMap.of(TIME, (Object) 2000, TEXT, "Bob owns the world"), false);
            remove(store, "doc1", ImmutableMap.of(TIME, (Object) 1001), false);
            add(store, "doc1", ImmutableMap.of(TIME, (Object) 1005, WEIGHT, 11.1), false);


        }

        clopen();

        for (String store : stores) {

            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 6.0))));
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00))));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "tomorrow"), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)))));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of(TIME, Cmp.LESS_THAN, 1010), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
            assertEquals(ImmutableSet.of("doc1", "doc4"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 10.0))));
            assertEquals(ImmutableSet.of("doc1", "doc4"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0))));
            assertEquals(0, result.size());

        }

    }

    private static String padNumber(long number) {
        String s = Long.toString(number);
        while (s.length()<18) s = "0"+s;
        return s;
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

//        List<String> result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.INTERVAL, Interval.of(0.2,0.3)))));
//        List<String> result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(LOCATION, Geo.WITHIN,Geoshape.circle(48.5,0.5,1000.00)))));
        long time = System.currentTimeMillis();
        List<String> result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of(WEIGHT, Cmp.LESS_THAN, 0.6), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
        int oldresultSize = result.size();
        System.out.println(result.size() + " vs " + (numDoc / 1000 * 2.4622623015));
        System.out.println("Query time on " + numDoc + " docs (ms): " + (System.currentTimeMillis() - time));
        result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of(WEIGHT, Cmp.LESS_THAN, 0.6), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))), numDoc / 1000));
        assertEquals(numDoc / 1000, result.size());
        result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of(WEIGHT, Cmp.LESS_THAN, 0.6), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))), numDoc / 1000 * 100));
        assertEquals(oldresultSize, result.size());
    }

    private void initialize(String store) throws StorageException {
        for (Map.Entry<String,KeyInformation> info : allKeys.entrySet()) {
            if (index.supports(info.getValue())) index.register(store,info.getKey(),info.getValue(),tx);
        }
    }

    private void add(String store, String docid, Map<String, Object> doc, boolean isNew) {
        for (Map.Entry<String, Object> kv : doc.entrySet()) {
            if (index.supports(allKeys.get(kv.getKey()))) {
                tx.add(store, docid, kv.getKey(), kv.getValue(), isNew);
            }
        }
    }

    private void remove(String store, String docid, Map<String, Object> doc, boolean deleteAll) {
        for (Map.Entry<String, Object> kv : doc.entrySet()) {
            if (index.supports(allKeys.get(kv.getKey()))) {
                tx.delete(store, docid, kv.getKey(), deleteAll);
            }
        }
    }


    public static Map<String, Object> getDocument(final String txt, final long time, final double weight, final Geoshape geo) {
        return new HashMap<String, Object>() {{
            put(TEXT, txt);
            put(NAME, txt);
            put(TIME, time);
            put(WEIGHT, weight);
            put(LOCATION, geo);
        }};
    }

    public static Map<String, Object> getRandomDocument() {
        final StringBuilder s = new StringBuilder();
        for (int i = 0; i < 3; i++) s.append(RandomGenerator.randomString(5, 8)).append(" ");
        return new HashMap<String, Object>() {{
            put(TEXT, s.toString());
            put(NAME, s.toString());
            put(TIME, Math.abs(random.nextLong()));
            put(WEIGHT, random.nextDouble());
            put(LOCATION, Geoshape.point(random.nextDouble() * 180 - 90, random.nextDouble() * 360 - 180));
        }};
    }

    public static void printResult(Iterable<RawQuery.Result<String>> result) {
        for (RawQuery.Result<String> r : result) {
            System.out.println(r.getResult() + ":"+r.getScore());
        }
    }

}
