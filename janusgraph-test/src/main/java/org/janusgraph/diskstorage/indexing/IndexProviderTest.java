// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.attribute.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;

import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.testutil.RandomGenerator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class IndexProviderTest {

    private static final Random random = new Random();
    private static final Parameter[] NO_PARAS = new Parameter[0];

    protected IndexProvider index;
    protected IndexFeatures indexFeatures;
    protected IndexTransaction tx;

    protected Map<String,KeyInformation> allKeys;
    protected KeyInformation.IndexRetriever indexRetriever;

    public static final String TEXT = "text", TIME = "time", WEIGHT = "weight", LOCATION = "location", NAME = "name", PHONE_LIST = "phone_list", PHONE_SET = "phone_set", DATE = "date";

    public static StandardKeyInformation of(Class<?> clazz, Cardinality cardinality,  Parameter... paras) {
        return new StandardKeyInformation(clazz, cardinality, paras);
    }

    public static final KeyInformation.IndexRetriever getIndexRetriever(final Map<String,KeyInformation> mappings) {
        return new KeyInformation.IndexRetriever() {

            @Override
            public KeyInformation get(String store, String key) {
                //Same for all stores
                return mappings.get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(String store) {
                return new KeyInformation.StoreRetriever() {
                    @Override
                    public KeyInformation get(String key) {
                        return mappings.get(key);
                    }
                };
            }
        };
    }

    public static final Map<String,KeyInformation> getMapping(final IndexFeatures indexFeatures) {
        Preconditions.checkArgument(indexFeatures.supportsStringMapping(Mapping.TEXTSTRING) ||
                (indexFeatures.supportsStringMapping(Mapping.TEXT) && indexFeatures.supportsStringMapping(Mapping.STRING)),
                "Index must support string and text mapping");
        return new HashMap<String,KeyInformation>() {{
            put(TEXT,new StandardKeyInformation(String.class, Cardinality.SINGLE, new Parameter("mapping",
                    indexFeatures.supportsStringMapping(Mapping.TEXT)?Mapping.TEXT:Mapping.TEXTSTRING)));
            put(TIME,new StandardKeyInformation(Long.class, Cardinality.SINGLE));
            put(WEIGHT,new StandardKeyInformation(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.DEFAULT)));
            put(LOCATION,new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE));
            put(NAME,new StandardKeyInformation(String.class, Cardinality.SINGLE, new Parameter("mapping",
                    indexFeatures.supportsStringMapping(Mapping.STRING)?Mapping.STRING:Mapping.TEXTSTRING)));
            if(indexFeatures.supportsCardinality(Cardinality.LIST)) {
                put(PHONE_LIST, new StandardKeyInformation(String.class, Cardinality.LIST, new Parameter("mapping",
                        indexFeatures.supportsStringMapping(Mapping.STRING) ? Mapping.STRING : Mapping.TEXTSTRING)));
            }
            if(indexFeatures.supportsCardinality(Cardinality.SET)) {
                put(PHONE_SET, new StandardKeyInformation(String.class, Cardinality.SET, new Parameter("mapping",
                        indexFeatures.supportsStringMapping(Mapping.STRING) ? Mapping.STRING : Mapping.TEXTSTRING)));
            }
            put(DATE,new StandardKeyInformation(Instant.class, Cardinality.SINGLE));
        }};
    }

    public abstract IndexProvider openIndex() throws BackendException;

    public abstract boolean supportsLuceneStyleQueries();

    @Before
    public void setUp() throws Exception {
        index = openIndex();
        index.clearStorage();
        index.close();
        open();
    }

    public void open() throws BackendException {
        index = openIndex();
        indexFeatures = index.getFeatures();
        allKeys = getMapping(indexFeatures);
        indexRetriever = getIndexRetriever(allKeys);

        newTx();
    }

    public void newTx() throws BackendException {
        if (tx != null) tx.commit();
        tx = openTx();
    }

    public IndexTransaction openTx() throws BackendException {
        BaseTransactionConfig config = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        return new IndexTransaction(index, indexRetriever, config, Duration.ofMillis(2000L));
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws BackendException {
        if (tx != null) tx.commit();
        index.close();
    }

    public void clopen() throws BackendException {
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

        Multimap<String, Object> doc1 = getDocument("Hello world", 1001, 5.2, Geoshape.point(48.0, 0.0), Arrays.asList("1", "2", "3"), Sets.newHashSet("1", "2"), Instant.ofEpochSecond(1));
        Multimap<String, Object> doc2 = getDocument("Tomorrow is the world", 1010, 8.5, Geoshape.point(49.0, 1.0), Arrays.asList("4", "5", "6"), Sets.newHashSet("4", "5"), Instant.ofEpochSecond(2));
        Multimap<String, Object> doc3 = getDocument("Hello Bob, are you there?", -500, 10.1, Geoshape.point(47.0, 10.0), Arrays.asList("7", "8", "9"), Sets.newHashSet("7", "8"), Instant.ofEpochSecond(3));

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
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "Tomorrow world"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "WorLD HELLO"))).size());


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

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.SINGLE), Text.CONTAINS_REGEX)) {
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "he[l]+(.*)")));
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "[h]+e[l]+(.*)")));
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "he[l]+")));
                assertTrue(result.isEmpty());
                result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "e[l]+(.*)")));
                assertTrue(result.isEmpty());
            }
            for (JanusGraphPredicate tp : new Text[]{Text.PREFIX, Text.REGEX}) {
                try {
                    assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, tp, "tzubull"))).size());
                    if (indexFeatures.supportsStringMapping(Mapping.TEXT)) fail();
                } catch (IllegalArgumentException e) {}
            }
            //String
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "Tomorrow is the world"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "world"))).size());
            assertEquals(3, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.NOT_EQUAL, "bob"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.PREFIX, "Tomorrow"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.PREFIX, "wor"))).size());
            for (JanusGraphPredicate tp : new Text[]{Text.CONTAINS,Text.CONTAINS_PREFIX, Text.CONTAINS_REGEX}) {
                try {
                    assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, tp, "tzubull"))).size());
                    if (indexFeatures.supportsStringMapping(Mapping.STRING)) fail();
                } catch (IllegalArgumentException e) {}
            }
            if (index.supports(new StandardKeyInformation(String.class, Cardinality.SINGLE), Text.REGEX)) {
                assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomo[r]+ow is.*world"))).size());
                assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomorrow"))).size());
            }

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.REGEX)) {
                assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomo[r]+ow is.*world"))).size());
                assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomorrow"))).size());
            }

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(TEXT, Text.CONTAINS, "hello"))));
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.query(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.EQUAL, -500)));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(Or.of(PredicateCondition.of(TIME, Cmp.EQUAL, 1001),PredicateCondition.of(TIME, Cmp.EQUAL, -500)))));
            assertEquals(2, result.size());

            result = tx.query(new IndexQuery(store, Not.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"))));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.EQUAL, -500), Not.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world")))));
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.query(new IndexQuery(store, And.of(Or.of(PredicateCondition.of(TIME, Cmp.EQUAL, 1001),PredicateCondition.of(TIME, Cmp.EQUAL, -500)), PredicateCondition.of(TEXT, Text.CONTAINS, "world"))));
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

            result = tx.query(new IndexQuery(store, PredicateCondition.of("location", Geo.WITHIN, Geoshape.box(46.5, -0.5, 50.5, 10.5))));
            assertEquals(3,result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2", "doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of(TIME, Cmp.LESS_THAN, 1010), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 10.0))));
            assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0))));
            assertEquals(0, result.size());

            if (supportsLuceneStyleQueries()) {
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:\"Hello Bob\"",NO_PARAS))));
                assertEquals(0, Iterables.size(tx.query(new RawQuery(store,"text:\"Hello Bob\"",NO_PARAS).setOffset(1))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:(world AND tomorrow)",NO_PARAS))));
//                printResult(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS)));
                assertEquals(2, Iterables.size(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS).setLimit(1))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS).setLimit(1).setOffset(1))));
                assertEquals(0, Iterables.size(tx.query(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS).setLimit(1).setOffset(2))));
                assertEquals(2, Iterables.size(tx.query(new RawQuery(store,"text:\"world\"",NO_PARAS))));
                assertEquals(2, Iterables.size(tx.query(new RawQuery(store,"time:[1000 TO 1020]",NO_PARAS))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"text:world AND time:1001",NO_PARAS))));
                assertEquals(1, Iterables.size(tx.query(new RawQuery(store,"name:\"Hello world\"",NO_PARAS))));
            }

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.LIST, new Parameter("mapping", Mapping.STRING)), Cmp.EQUAL)) {
                assertEquals("doc1", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "1"))).get(0));
                assertEquals("doc1", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "2"))).get(0));
                assertEquals("doc2", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "4"))).get(0));
                assertEquals("doc2", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "5"))).get(0));
                assertEquals("doc3", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "7"))).get(0));
                assertEquals("doc3", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "8"))).get(0));
                assertEquals("doc1", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "1"))).get(0));
                assertEquals("doc1", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "2"))).get(0));
                assertEquals("doc2", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "4"))).get(0));
                assertEquals("doc2", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "5"))).get(0));
                assertEquals("doc3", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "7"))).get(0));
                assertEquals("doc3", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "8"))).get(0));

            }

            assertEquals("doc1", tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(1)))).get(0));
            assertEquals("doc2", tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(2)))).get(0));
            assertEquals("doc3", tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(3)))).get(0));
            assertEquals("doc3", tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.GREATER_THAN, Instant.ofEpochSecond(2)))).get(0));
            assertEquals(ImmutableSet.of("doc2", "doc3"), ImmutableSet.copyOf(tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.GREATER_THAN_EQUAL, Instant.ofEpochSecond(2))))));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.LESS_THAN, Instant.ofEpochSecond(2))))));
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.LESS_THAN_EQUAL, Instant.ofEpochSecond(2))))));
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.NOT_EQUAL, Instant.ofEpochSecond(2))))));


            //Update some data
            add(store, "doc4", getDocument("I'ts all a big Bob", -100, 11.2, Geoshape.point(48.0, 8.0), Arrays.asList("10", "11", "12"), Sets.newHashSet("10", "11"), Instant.ofEpochSecond(4)), true);
            remove(store, "doc2", doc2, true);
            remove(store, "doc3", ImmutableMultimap.of(WEIGHT, (Object) 10.1), false);
            add(store, "doc3", ImmutableMultimap.of(TIME, (Object) 2000, TEXT, "Bob owns the world"), false);
            remove(store, "doc1", ImmutableMultimap.of(TIME, (Object) 1001), false);
            add(store, "doc1", ImmutableMultimap.of(TIME, (Object) 1005, WEIGHT, 11.1), false);


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

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.LIST, new Parameter("mapping", Mapping.STRING)), Cmp.EQUAL)) {
                assertEquals("doc4", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "10"))).get(0));
                assertEquals("doc4", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "11"))).get(0));
                assertEquals("doc4", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "10"))).get(0));
                assertEquals("doc4", tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "11"))).get(0));
                assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "4"))).size());
                assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "5"))).size());
            }

            assertTrue(tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(2)))).isEmpty());
            assertEquals("doc4", tx.query(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(4)))).get(0));


        }

    }

    @Test
    public void testCommonSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING))));

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT))));

        assertTrue(index.supports(of(Long.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Long.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.DEFAULT))));
        assertTrue(index.supports(of(Integer.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Short.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Byte.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Float.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Object.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Exception.class, Cardinality.SINGLE)));

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.DEFAULT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT)), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.WITHIN));

        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE), Geo.INTERSECT));
        assertFalse(index.supports(of(Long.class, Cardinality.SINGLE), Text.CONTAINS));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.DISJOINT));
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

    @Test
    public void testRestore() throws Exception {
        final String store1 = "store1";
        final String store2 = "store2";

        initialize(store1);
        initialize(store2);

        // add couple of documents with weight > 4.0d
        add(store1, "restore-doc1", ImmutableMultimap.of(NAME, "first", TIME, 1L, WEIGHT, 10.2d), true);
        add(store1, "restore-doc2", ImmutableMultimap.of(NAME, "second", TIME, 2L, WEIGHT, 4.7d), true);

        clopen();

        // initial query
        Set<String> results = Sets.newHashSet(tx.query(new IndexQuery(store1, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(2, results.size());

        // now let's try to restore (change values on the existing doc2, delete doc1, and add a new doc)
        index.restore(new HashMap<String, Map<String, List<IndexEntry>>>() {{
            put(store1, new HashMap<String, List<IndexEntry>>() {{
                put("restore-doc1", Collections.<IndexEntry>emptyList());
                put("restore-doc2", new ArrayList<IndexEntry>() {{
                    add(new IndexEntry(NAME, "not-second"));
                    add(new IndexEntry(WEIGHT, 2.1d));
                    add(new IndexEntry(TIME, 0L));
                }});
                put("restore-doc3", new ArrayList<IndexEntry>() {{
                    add(new IndexEntry(NAME, "third"));
                    add(new IndexEntry(WEIGHT, 11.5d));
                    add(new IndexEntry(TIME, 3L));
                }});
            }});
        }}, indexRetriever, tx);

        clopen();

        // this should return only doc3 (let's make results a set so it filters out duplicates but still has a size)
        results = Sets.newHashSet(tx.query(new IndexQuery(store1, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc3"));

        // check if the name and time was set correctly for doc3
        results = Sets.newHashSet(tx.query(new IndexQuery(store1, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "third"), PredicateCondition.of(TIME, Cmp.EQUAL, 3L)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc3"));

        // let's check if all of the new properties where set correctly from doc2
        results = Sets.newHashSet(tx.query(new IndexQuery(store1, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "not-second"), PredicateCondition.of(TIME, Cmp.EQUAL, 0L)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc2"));

        // now let's throw one more store in the mix (resurrect doc1 in store1 and add it to the store2)
        index.restore(new HashMap<String, Map<String, List<IndexEntry>>>() {{
            put(store1, new HashMap<String, List<IndexEntry>>() {{
                put("restore-doc1", new ArrayList<IndexEntry>() {{
                    add(new IndexEntry(NAME, "first-restored"));
                    add(new IndexEntry(WEIGHT, 7.0d));
                    add(new IndexEntry(TIME, 4L));
                }});
            }});
            put(store2, new HashMap<String, List<IndexEntry>>() {{
                put("restore-doc1", new ArrayList<IndexEntry>() {{
                    add(new IndexEntry(NAME, "first-in-second-store"));
                    add(new IndexEntry(WEIGHT, 4.0d));
                    add(new IndexEntry(TIME, 5L));
                }});
            }});
        }}, indexRetriever, tx);

        clopen();

        // let's query store1 to see if we got doc1 back
        results = Sets.newHashSet(tx.query(new IndexQuery(store1, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(2, results.size());
        assertTrue(results.contains("restore-doc1"));
        assertTrue(results.contains("restore-doc3"));

        // check if the name and time was set correctly for doc1
        results = Sets.newHashSet(tx.query(new IndexQuery(store1, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "first-restored"), PredicateCondition.of(TIME, Cmp.EQUAL, 4L)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc1"));

        // now let's check second store and see if we got doc1 added there too
        results = Sets.newHashSet(tx.query(new IndexQuery(store2, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc1"));

        // check if the name and time was set correctly for doc1 (in second store)
        results = Sets.newHashSet(tx.query(new IndexQuery(store2, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "first-in-second-store"), PredicateCondition.of(TIME, Cmp.EQUAL, 5L)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc1"));
    }

    @Test
    public void testTTL() throws Exception {
        if (!index.getFeatures().supportsDocumentTTL())
            return;

        final String store = "store1";

        initialize(store);

        // add couple of documents with weight > 4.0d
        add(store, "expiring-doc1", ImmutableMultimap.of(NAME, "first", TIME, 1L, WEIGHT, 10.2d), true, 2);
        add(store, "expiring-doc2", ImmutableMultimap.of(NAME, "second", TIME, 2L, WEIGHT, 4.7d), true);
        add(store, "expiring-doc3", ImmutableMultimap.of(NAME, "third", TIME, 3L, WEIGHT, 5.2d), true, 2);
        add(store, "expiring-doc4", ImmutableMultimap.of(NAME, "fourth", TIME, 3L, WEIGHT, 7.7d), true, 7);// bigger ttl then one recycle interval, should still show up in the results

        clopen();

        // initial query
        Set<String> results = Sets.newHashSet(tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(4, results.size());

        Thread.sleep(6000); // sleep for elastic search ttl recycle

        results = Sets.newHashSet(tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(2, results.size());
        assertTrue(results.contains("expiring-doc2"));
        assertTrue(results.contains("expiring-doc4"));

        Thread.sleep(5000); // sleep for elastic search ttl recycle
        results = Sets.newHashSet(tx.query(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0)))));
        assertEquals(1, results.size());
        assertTrue(results.contains("expiring-doc2"));
    }

   /* ==================================================================================
                            CONCURRENT UPDATE CASES
     ==================================================================================*/


    private final String defStore = "store1";
    private final String defDoc = "docx1";
    private final String defTextValue = "the quick brown fox jumps over the lazy dog";

    private interface TxJob {
        void run(IndexTransaction tx);
    }

    private void runConflictingTx(TxJob job1, TxJob job2) throws Exception {
        initialize(defStore);
        Multimap<String, Object> initialProps = ImmutableMultimap.of(TEXT, defTextValue);
        add(defStore, defDoc, initialProps, true);
        clopen();

        // Sanity check
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),defDoc);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "periwinkle")),null);

        IndexTransaction tx1 = openTx(), tx2 = openTx();
        job1.run(tx1);
        tx1.commit();
        job2.run(tx2);
        tx2.commit();

        clopen();
    }

    private void checkResult(IndexQuery query, String containedDoc) throws Exception {
        List<String> result = tx.query(query);
        if (containedDoc!=null) {
            assertEquals(1, result.size());
            assertEquals(containedDoc, result.get(0));
        } else {
            assertEquals(0, result.size());
        }
    }


    @Test
    public void testDeleteDocumentThenDeleteField() throws Exception {
        runConflictingTx(new TxJob() {
            @Override
            public void run(IndexTransaction tx) {
                tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true);
            }
        }, new TxJob() {
             @Override
             public void run(IndexTransaction tx) {
                 tx.delete(defStore, defDoc, TEXT, defTextValue, false);
             }
         });

        // Document must not exist
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
    }

    @Test
    public void testDeleteDocumentThenModifyField() throws Exception {
        runConflictingTx(new TxJob() {
            @Override
            public void run(IndexTransaction tx) {
                tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true);
            }
        }, new TxJob() {
            @Override
            public void run(IndexTransaction tx) {
                tx.add(defStore, defDoc, TEXT, "the slow brown fox jumps over the lazy dog", false);
            }
        });

        //2nd tx should put document back into existence
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),defDoc);
    }

    @Test
    public void testDeleteDocumentThenAddField() throws Exception {
        final String nameValue = "jm keynes";

        runConflictingTx(new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true);
                             }
                         }, new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 tx.add(defStore, defDoc, NAME, nameValue, false);
                             }
                         });

        // TEXT field should have been deleted when document was
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
        // but name field should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(NAME, Cmp.EQUAL, nameValue)),defDoc);
    }

    @Test
    public void testAddFieldThenDeleteDoc() throws Exception {
        final String nameValue = "jm keynes";

        runConflictingTx(new TxJob() {
            @Override
            public void run(IndexTransaction tx) {
                tx.add(defStore, defDoc, NAME, nameValue, false);
            }
        }, new TxJob() {
            @Override
            public void run(IndexTransaction tx) {
                tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true);
            }
        });

        //neither should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(NAME, Cmp.EQUAL, nameValue)),null);
    }

    @Test
    public void testConflictingAdd() throws Exception {
        final String doc2 = "docy2";
        runConflictingTx(new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 Multimap<String, Object> initialProps = ImmutableMultimap.<String, Object>of(TEXT, "sugar sugar");
                                 add(defStore, doc2, initialProps, true);
                             }
                         }, new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 Multimap<String, Object> initialProps = ImmutableMultimap.<String, Object>of(TEXT, "honey honey");
                                 add(defStore, doc2, initialProps, true);
                             }
                         });

        //only last write should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),defDoc);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "sugar")),null);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "honey")),doc2);
    }

    @Test
    public void testLastWriteWins() throws Exception {
        runConflictingTx(new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 tx.delete(defStore, defDoc, TEXT, defTextValue, false);
                                 tx.add(defStore, defDoc, TEXT, "sugar sugar", false);
                             }
                         }, new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 tx.delete(defStore, defDoc, TEXT, defTextValue, false);
                                 tx.add(defStore, defDoc, TEXT, "honey honey", false);
                             }
                         });

        //only last write should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "sugar")),null);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "honey")),defDoc);
    }

    /**
     * Test overwriting a single existing field on an existing document
     * (isNew=false). Non-contentious test.
     *
     */
    @Test
    public void testUpdateAddition() throws Exception {
        final String revisedText = "its a sunny day";
        runConflictingTx(new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 tx.add(defStore, defDoc, TEXT, revisedText, false);
                             }
                         }, new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 //do nothing
                             }
                         });

        // Should no longer return old text
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")), null);
        // but new one
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "sunny")),defDoc);
    }

    /**
     * Test deleting a single field from a single document (deleteAll=false).
     * Non-contentious test.
     *
     */
    @Test
    public void testUpdateDeletion() throws Exception {
        runConflictingTx(new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), false);
                             }
                         }, new TxJob() {
                             @Override
                             public void run(IndexTransaction tx) {
                                 //do nothing
                             }
                         });

        // Should no longer return deleted text
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
    }

    /* ==================================================================================
                            HELPER METHODS
     ==================================================================================*/


    protected void initialize(String store) throws BackendException {
        for (Map.Entry<String,KeyInformation> info : allKeys.entrySet()) {
            KeyInformation keyInfo = info.getValue();
            if (index.supports(keyInfo)) index.register(store,info.getKey(),keyInfo,tx);
        }
    }

    protected void add(String store, String docid, Multimap<String, Object> doc, boolean isNew) {
        add(store, docid, doc, isNew, 0);
    }

    private void add(String store, String docid, Multimap<String, Object> doc, boolean isNew, int ttlInSeconds) {
        for (Map.Entry<String, Object> kv : doc.entries()) {
            if (!index.supports(allKeys.get(kv.getKey())))
                continue;

            IndexEntry idx = new IndexEntry(kv.getKey(), kv.getValue());
            if (ttlInSeconds > 0)
                idx.setMetaData(EntryMetaData.TTL, ttlInSeconds);

            tx.add(store, docid, idx, isNew);
        }
    }

    private void remove(String store, String docid, Multimap<String, Object> doc, boolean deleteAll) {
        for (Map.Entry<String, Object> kv : doc.entries()) {
            if (index.supports(allKeys.get(kv.getKey()))) {
                tx.delete(store, docid, kv.getKey(), kv.getValue(), deleteAll);
            }
        }
    }


    public Multimap<String, Object> getDocument(final String txt, final long time, final double weight, final Geoshape geo, List<String> phoneList, Set<String> phoneSet, Instant date) {
        HashMultimap<String, Object> values = HashMultimap.create();
        values.put(TEXT, txt);
        values.put(NAME, txt);
        values.put(TIME, time);
        values.put(WEIGHT, weight);
        values.put(LOCATION, geo);
        values.put(DATE, date);
        if(indexFeatures.supportsCardinality(Cardinality.LIST)) {
            for (String phone : phoneList) {
                values.put(PHONE_LIST, phone);
            }
        }
        if(indexFeatures.supportsCardinality(Cardinality.SET)) {
            for (String phone : phoneSet) {
                values.put(PHONE_SET, phone);
            }
        }
        return values;
    }

    public static Multimap<String, Object> getRandomDocument() {
        final StringBuilder s = new StringBuilder();
        for (int i = 0; i < 3; i++) s.append(RandomGenerator.randomString(5, 8)).append(" ");
        Multimap values = HashMultimap.create();

        values.put(TEXT, s.toString());
        values.put(NAME, s.toString());
        values.put(TIME, Math.abs(random.nextLong()));
        values.put(WEIGHT, random.nextDouble());
        values.put(LOCATION, Geoshape.point(random.nextDouble() * 180 - 90, random.nextDouble() * 360 - 180));
        return values;
    }

    public static void printResult(Iterable<RawQuery.Result<String>> result) {
        for (RawQuery.Result<String> r : result) {
            System.out.println(r.getResult() + ":"+r.getScore());
        }
    }

}
