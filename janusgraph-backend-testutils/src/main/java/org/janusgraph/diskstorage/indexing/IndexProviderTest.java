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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.testutil.RandomGenerator;
import org.junit.Assume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class IndexProviderTest {

    private static final Random random = new Random();
    private static final Parameter<?>[] NO_PARAS = new Parameter[0];

    protected IndexProvider index;
    protected IndexFeatures indexFeatures;
    protected IndexTransaction tx;

    protected Map<String,KeyInformation> allKeys;
    protected KeyInformation.IndexRetriever indexRetriever;

    public static final String TEXT = "text", TIME = "time", WEIGHT = "weight", LOCATION = "location",
            BOUNDARY = "boundary", NAME = "name", PHONE_LIST = "phone_list", PHONE_SET = "phone_set", DATE = "date", TIME_TICK = "time_tick",
            STRING="string", ANALYZED="analyzed", FULL_TEXT="full_text", KEYWORD="keyword", TEXT_STRING="text_string", BOOLEAN="boolean";

    public static StandardKeyInformation of(Class<?> clazz, Cardinality cardinality,  Parameter<?>... paras) {
        return new StandardKeyInformation(clazz, cardinality, paras);
    }

    public static KeyInformation.IndexRetriever getIndexRetriever(final Map<String,KeyInformation> mappings) {
        return new KeyInformation.IndexRetriever() {

            @Override
            public KeyInformation get(String store, String key) {
                //Same for all stores
                return mappings.get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(String store) {
                return mappings::get;
            }

            @Override
            public void invalidate(String store) {
                mappings.remove(store);
            }
        };
    }

    public static Map<String,KeyInformation> getMapping(final IndexFeatures indexFeatures, final String englishAnalyzerName, final String keywordAnalyzerName) {
        Preconditions.checkArgument(indexFeatures.supportsStringMapping(Mapping.TEXTSTRING) ||
                (indexFeatures.supportsStringMapping(Mapping.TEXT) && indexFeatures.supportsStringMapping(Mapping.STRING)),
                "Index must support string and text mapping");
        final Parameter<?> textParameter = indexFeatures.supportsStringMapping(Mapping.TEXT) ? Mapping.TEXT.asParameter() : Mapping.TEXTSTRING.asParameter();
        final Parameter<?> stringParameter = indexFeatures.supportsStringMapping(Mapping.STRING) ? Mapping.STRING.asParameter() : Mapping.TEXTSTRING.asParameter();
        return new HashMap<String, KeyInformation>() {{
            put(BOOLEAN, new StandardKeyInformation(Boolean.class, Cardinality.SINGLE));
            put(TEXT, new StandardKeyInformation(String.class, Cardinality.SINGLE, textParameter));
            put(TIME, new StandardKeyInformation(Long.class, Cardinality.SINGLE));
            put(WEIGHT, new StandardKeyInformation(Double.class, Cardinality.SINGLE, Mapping.DEFAULT.asParameter()));
            put(LOCATION, new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE));
            put(BOUNDARY, new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE, Mapping.PREFIX_TREE.asParameter()));
            put(NAME, new StandardKeyInformation(String.class, Cardinality.SINGLE, stringParameter));
            if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
                put(PHONE_LIST, new StandardKeyInformation(String.class, Cardinality.LIST, stringParameter));
                put(TIME_TICK, new StandardKeyInformation(Date.class, Cardinality.LIST));
            }
            if(indexFeatures.supportsCardinality(Cardinality.SET)) {
                put(PHONE_SET, new StandardKeyInformation(String.class, Cardinality.SET, stringParameter));
            }
            put(DATE,new StandardKeyInformation(Instant.class, Cardinality.SINGLE));
            put(STRING, new StandardKeyInformation(String.class, Cardinality.SINGLE, stringParameter, new Parameter<>(ParameterType.STRING_ANALYZER.getName(), englishAnalyzerName)));
            put(ANALYZED, new StandardKeyInformation(String.class, Cardinality.SINGLE, textParameter, new Parameter<>(ParameterType.TEXT_ANALYZER.getName(), englishAnalyzerName)));
            if(indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)){
                put(FULL_TEXT, new StandardKeyInformation(String.class, Cardinality.SINGLE,
                        Mapping.TEXTSTRING.asParameter(), new Parameter<>(ParameterType.STRING_ANALYZER.getName(), englishAnalyzerName),
                    new Parameter<>(ParameterType.TEXT_ANALYZER.getName(), englishAnalyzerName)));
                put(TEXT_STRING, new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.TEXTSTRING.asParameter()));
            }
            put(KEYWORD, new StandardKeyInformation(String.class, Cardinality.SINGLE, textParameter, new Parameter<>(ParameterType.TEXT_ANALYZER.getName(), keywordAnalyzerName)));
        }};
    }

    public abstract IndexProvider openIndex() throws BackendException;

    public abstract boolean supportsLuceneStyleQueries();

    public abstract String getEnglishAnalyzerName();

    public abstract String getKeywordAnalyzerName();

    @BeforeEach
    public void setUp() throws Exception {
        index = openIndex();
        index.clearStorage();
        index.close();
        open();
    }

    public void open() throws BackendException {
        index = openIndex();
        indexFeatures = index.getFeatures();
        allKeys = getMapping(indexFeatures, getEnglishAnalyzerName(), getKeywordAnalyzerName());
        indexRetriever = getIndexRetriever(allKeys);

        newTx();
    }

    public void newTx() throws BackendException {
        if (tx != null) tx.commit();
        tx = openTx();
    }

    public IndexTransaction openTx() throws BackendException {
        final BaseTransactionConfig config = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        return new IndexTransaction(index, indexRetriever, config, Duration.ofMillis(2000L));
    }

    @AfterEach
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

        final Multimap<String, Object> doc1 = getDocument("Hello world", 1001, 5.2, Geoshape.point(48.0, 0.0), Geoshape.polygon(Arrays.asList(new double[][]{{-0.1, 47.9}, {0.1, 47.9}, {0.1, 48.1}, {-0.1, 48.1}, {-0.1, 47.9}})), Arrays.asList("1", "2", "3"), Sets.newHashSet("1", "2"), Instant.ofEpochSecond(1),
            false);
        final Multimap<String, Object> doc2 = getDocument("Tomorrow is the world", 1010, 8.5, Geoshape.point(49.0, 1.0), Geoshape.line(Arrays.asList(new double[][]{{0.9, 48.9}, {0.9, 49.1}, {1.1, 49.1}, {1.1, 48.9}})), Arrays.asList("4", "5", "6"), Sets.newHashSet("4", "5"), Instant.ofEpochSecond(2),
            true);
        final Multimap<String, Object> doc3 = getDocument("Hello Bob, are you there?", -500, 10.1, Geoshape.point(47.0, 10.0), Geoshape.box(46.9, 9.9, 47.1, 10.1), Arrays.asList("7", "8", "9"), Sets.newHashSet("7", "8"), Instant.ofEpochSecond(3),
            false);

        for (final String store : stores) {
            initialize(store);

            add(store, "doc1", doc1, true);
            add(store, "doc2", doc2, true);
            add(store, "doc3", doc3, false);

        }

        final ImmutableList<IndexQuery.OrderEntry> orderTimeAsc = ImmutableList.of(new IndexQuery.OrderEntry(TIME, Order.ASC, Integer.class));
        final ImmutableList<IndexQuery.OrderEntry> orderWeightAsc = ImmutableList.of(new IndexQuery.OrderEntry(WEIGHT, Order.ASC, Double.class));
        final ImmutableList<IndexQuery.OrderEntry> orderTimeDesc = ImmutableList.of(new IndexQuery.OrderEntry(TIME, Order.DESC, Integer.class));
        final ImmutableList<IndexQuery.OrderEntry> orderWeightDesc = ImmutableList.of(new IndexQuery.OrderEntry(WEIGHT, Order.DESC, Double.class));
        final ImmutableList<IndexQuery.OrderEntry> jointOrder = ImmutableList.of(new IndexQuery.OrderEntry(WEIGHT, Order.DESC, Double.class), new IndexQuery.OrderEntry(TIME, Order.DESC, Integer.class));
        final ImmutableList<IndexQuery.OrderEntry> orderNameAsc = ImmutableList.of(new IndexQuery.OrderEntry(NAME, Order.ASC, String.class));
        final ImmutableList<IndexQuery.OrderEntry> orderNameDesc = ImmutableList.of(new IndexQuery.OrderEntry(NAME, Order.DESC, String.class));
        final ImmutableList<IndexQuery.OrderEntry> orderDateAsc = ImmutableList.of(new IndexQuery.OrderEntry(DATE, Order.ASC, Instant.class));
        final ImmutableList<IndexQuery.OrderEntry> orderDateDesc = ImmutableList.of(new IndexQuery.OrderEntry(DATE, Order.DESC, Instant.class));
        final ImmutableList<IndexQuery.OrderEntry> orderBooleanDesc = ImmutableList.of(new IndexQuery.OrderEntry(BOOLEAN, Order.DESC, Boolean.class));
        final ImmutableList<IndexQuery.OrderEntry> orderBooleanAsc = ImmutableList.of(new IndexQuery.OrderEntry(BOOLEAN, Order.ASC, Boolean.class));

        clopen();

        for (final String store : stores) {
            //Token
            List<String> result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            assertEquals(ImmutableSet.copyOf(result), tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "wOrLD"))).collect(Collectors.toSet()));
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "worl"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "Tomorrow world"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "WorLD HELLO"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_FUZZY, "boby"))).count());

            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.GREATER_THAN, "A"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.GREATER_THAN, "z"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.GREATER_THAN, "world"))).count());

            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.GREATER_THAN_EQUAL, "A"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.GREATER_THAN_EQUAL, "z"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.GREATER_THAN_EQUAL, "world"))).count());

            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.LESS_THAN, "A"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.LESS_THAN, "z"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.LESS_THAN, "world"))).count());

            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.LESS_THAN_EQUAL, "A"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.LESS_THAN_EQUAL, "z"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Cmp.LESS_THAN_EQUAL, "world"))).count());

            //Ordering
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderTimeDesc))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new RawQuery(store, "text:\"world\"", orderTimeDesc, NO_PARAS))
                                                      .map(RawQuery.Result::getResult)
                                                      .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderWeightDesc))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new RawQuery(store, "text:\"world\"", orderWeightDesc, NO_PARAS))
                       .map(RawQuery.Result::getResult)
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderTimeAsc))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.queryStream(new RawQuery(store, "text:\"world\"", orderTimeAsc, NO_PARAS))
                       .map(RawQuery.Result::getResult)
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderWeightAsc))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.queryStream(new RawQuery(store, "text:\"world\"", orderWeightAsc, NO_PARAS))
                       .map(RawQuery.Result::getResult)
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), jointOrder))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderNameAsc))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderNameDesc))
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderDateAsc))
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderDateDesc))
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderBooleanDesc))
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc2", "doc1"), result);
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world"), orderBooleanAsc))
                       .collect(Collectors.toList());
            assertEquals(ImmutableList.of("doc1", "doc2"), result);

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_PREFIX, "w")))
                    .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_PREFIX, "wOr"))).collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));
            assertEquals(0,tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_PREFIX, "bobi"))).count());

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.SINGLE), Text.CONTAINS_REGEX)) {
                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "he[l]+(.*)"))).collect(Collectors.toList());
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "[h]+e[l]+(.*)"))).collect(Collectors.toList());
                assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));
                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "he[l]+"))).collect(Collectors.toList());
                assertTrue(result.isEmpty());
                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS_REGEX, "e[l]+(.*)"))).collect(Collectors.toList());
                assertTrue(result.isEmpty());
            }
            for (final JanusGraphPredicate tp : new Text[]{Text.PREFIX, Text.REGEX}) {
                try {
                    assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, tp, "tzubull"))).count());
                    if (indexFeatures.supportsStringMapping(Mapping.TEXT)) fail();
                } catch (final IllegalArgumentException ignored) {}
            }
            //String
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "Tomorrow is the world"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "world"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.NOT_EQUAL, "bob"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.PREFIX, "Tomorrow"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.PREFIX, "wor"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.FUZZY, "Tomorow is the world"))).count());

            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.GREATER_THAN, "A"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.GREATER_THAN, "z"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.GREATER_THAN, "Hello world"))).count());

            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.GREATER_THAN_EQUAL, "A"))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.GREATER_THAN_EQUAL, "z"))).count());
            assertEquals(2, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.GREATER_THAN_EQUAL, "Hello world"))).count());

            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.LESS_THAN, "A"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.LESS_THAN, "z"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.LESS_THAN, "Hello world"))).count());

            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.LESS_THAN_EQUAL, "A"))).count());
            assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.LESS_THAN_EQUAL, "z"))).count());
            assertEquals(2, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.LESS_THAN_EQUAL, "Hello world"))).count());

            try {
                tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Mockito.mock(Cmp.class), "value")));
                fail("should fail");
            } catch (final IllegalArgumentException ignored) {
            }

            for (final JanusGraphPredicate tp : new Text[]{Text.CONTAINS,Text.CONTAINS_PREFIX, Text.CONTAINS_REGEX}) {
                try {
                    assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, tp, "tzubull"))).count());
                    if (indexFeatures.supportsStringMapping(Mapping.STRING)) fail();
                } catch (final IllegalArgumentException ignored) {}
            }
            if (index.supports(new StandardKeyInformation(String.class, Cardinality.SINGLE), Text.REGEX)) {
                assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomo[r]+ow is.*world"))).count());
                assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomorrow"))).count());
            }

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.REGEX)) {
                assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomo[r]+ow is.*world"))).count());
                assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Text.REGEX, "Tomorrow"))).count());
            }

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(TEXT, Text.CONTAINS, "hello")))).collect(Collectors.toList());

            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.EQUAL, -500))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.queryStream(new IndexQuery(store, And.of(Or.of(PredicateCondition.of(TIME, Cmp.EQUAL, 1001),PredicateCondition.of(TIME, Cmp.EQUAL, -500))))).collect(Collectors.toList());
            assertEquals(2, result.size());

            result = tx.queryStream(new IndexQuery(store, Not.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world")))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.EQUAL, -500), Not.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"))))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.queryStream(new IndexQuery(store, And.of(Or.of(PredicateCondition.of(TIME, Cmp.EQUAL, 1001),PredicateCondition.of(TIME, Cmp.EQUAL, -500)), PredicateCondition.of(TEXT, Text.CONTAINS, "world")))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "Bob"))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "Bob")))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc3", result.get(0));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 6.0)))).collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc2", result.get(0));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.box(46.5, -0.5, 50.5, 10.5)))).collect(Collectors.toList());
            assertEquals(3,result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2", "doc3"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)))).collect(Collectors.toList());
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.box(46.5, -0.5, 50.5, 10.5)))).collect(Collectors.toList());
            assertEquals(3,result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2", "doc3"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)))).collect(Collectors.toList());
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.polygon(Arrays.asList(new double[][]
                    {{-5.0,47.0},{5.0,47.0},{5.0,50.0},{-5.0,50.0},{-5.0,47.0}}))))).collect(Collectors.toList());
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1","doc2"), ImmutableSet.copyOf(result));

            if (index.supports(new StandardKeyInformation(Geoshape.class, Cardinality.SINGLE, Mapping.PREFIX_TREE.asParameter()), Geo.DISJOINT)) {
                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.DISJOINT, Geoshape.box(46.5, -0.5, 50.5, 10.5)))).collect(Collectors.toList());
                assertEquals(0,result.size());

                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.DISJOINT, Geoshape.circle(48.5, 0.5, 200.00)))).collect(Collectors.toList());
                assertEquals(1, result.size());
                assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));

                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.DISJOINT, Geoshape.polygon(Arrays.asList(new double[][]
                        {{-5.0,47.0},{5.0,47.0},{5.0,50.0},{-5.0,50.0},{-5.0,47.0}}))))).collect(Collectors.toList());
                assertEquals(1, result.size());
                assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));
            }

            if (indexFeatures.supportsGeoContains()) {
                result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.CONTAINS, Geoshape.point(47, 10)))).collect(Collectors.toList());
                assertEquals(1, result.size());
                assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));
            }

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.INTERSECT, Geoshape.box(48,-1,49,2)))).collect(Collectors.toList());
            assertEquals(2,result.size());
            assertEquals(ImmutableSet.of("doc1","doc2"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.INTERSECT, Geoshape.circle(48.5, 0.5, 200.00)))).collect(Collectors.toList());
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.INTERSECT, Geoshape.polygon(Arrays.asList(new double[][] {{-1.0,48.0},{2.0,48.0},{2.0,49.0},{-1.0,49.0},{-1.0,48.0}}))))).collect(Collectors.toList());
            assertEquals(2, result.size());
            assertEquals(ImmutableSet.of("doc1","doc2"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of("text", Text.CONTAINS, "tomorrow"), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00)), PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 200.00))))).collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc2"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of(TIME, Cmp.LESS_THAN, 1010), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)), PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))))).collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc3"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 10.0)))).collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc3"), ImmutableSet.copyOf(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0)))).collect(Collectors.toList());
            assertEquals(0, result.size());

            if (supportsLuceneStyleQueries()) {
                assertEquals(1, tx.queryStream(new RawQuery(store,"text:\"Hello Bob\"",NO_PARAS)).count());
                assertEquals(0, tx.queryStream(new RawQuery(store,"text:\"Hello Bob\"",NO_PARAS).setOffset(1)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"text:(world AND tomorrow)",NO_PARAS)).count());
                assertEquals(2, tx.queryStream(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS).setLimit(1)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS).setLimit(1).setOffset(1)).count());
                assertEquals(0, tx.queryStream(new RawQuery(store,"text:(you there Hello Bob)",NO_PARAS).setLimit(1).setOffset(2)).count());
                assertEquals(2, tx.queryStream(new RawQuery(store,"text:\"world\"",NO_PARAS)).count());
                assertEquals(2, tx.queryStream(new RawQuery(store,"time:[1000 TO 1020]",NO_PARAS)).count());
                assertEquals(2, tx.queryStream(new RawQuery(store,"time:[1000 TO *]",NO_PARAS)).count());
                assertEquals(3, tx.queryStream(new RawQuery(store,"time:[* TO *]",NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"weight:[5.1 TO 8.3]",NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"weight:5.2",NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"text:world AND time:1001",NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store,"name:\"Hello world\"",NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store, "boolean:true", NO_PARAS)).count());
                assertEquals(2, tx.queryStream(new RawQuery(store, "boolean:false", NO_PARAS)).count());
                assertEquals(2, tx.queryStream(new RawQuery(store, "date:{1970-01-01T00:00:01Z TO 1970-01-01T00:00:03Z]", NO_PARAS)).count());
                assertEquals(3, tx.queryStream(new RawQuery(store, "date:[1970-01-01T00:00:01Z TO *]", NO_PARAS)).count());
                assertEquals(1, tx.queryStream(new RawQuery(store, "date:\"1970-01-01T00:00:02Z\"", NO_PARAS)).count());
            }

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.LIST, Mapping.STRING.asParameter()), Cmp.EQUAL)) {
                assertEquals("doc1", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "1"))).findFirst().get());
                assertEquals("doc1", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "2"))).findFirst().get());
                assertEquals("doc2", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "4"))).findFirst().get());
                assertEquals("doc2", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "5"))).findFirst().get());
                assertEquals("doc3", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "7"))).findFirst().get());
                assertEquals("doc3", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "8"))).findFirst().get());
                assertEquals("doc1", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "1"))).findFirst().get());
                assertEquals("doc1", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "2"))).findFirst().get());
                assertEquals("doc2", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "4"))).findFirst().get());
                assertEquals("doc2", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "5"))).findFirst().get());
                assertEquals("doc3", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "7"))).findFirst().get());
                assertEquals("doc3", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "8"))).findFirst().get());

                remove(store, "doc1", ImmutableMultimap.of(PHONE_LIST, "1"), false);
                clopen();
                assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "1"))).count());
                assertEquals("doc1", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "2"))).findFirst().get());

                remove(store, "doc2", ImmutableMultimap.of(PHONE_SET, "4"), false);
                clopen();
                assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "4"))).count());
                assertEquals("doc2", tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "5"))).findFirst().get());
            }

            assertEquals("doc1", tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(1)))).findFirst().get());
            assertEquals("doc2", tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(2)))).findFirst().get());
            assertEquals("doc3", tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(3)))).findFirst().get());
            assertEquals("doc3", tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.GREATER_THAN, Instant.ofEpochSecond(2)))).findFirst().get());
            assertEquals(ImmutableSet.of("doc2", "doc3"), tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.GREATER_THAN_EQUAL, Instant.ofEpochSecond(2)))).collect(Collectors.toSet()));
            assertEquals(ImmutableSet.of("doc1"), tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.LESS_THAN, Instant.ofEpochSecond(2)))).collect(Collectors.toSet()));
            assertEquals(ImmutableSet.of("doc1", "doc2"), tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.LESS_THAN_EQUAL, Instant.ofEpochSecond(2)))).collect(Collectors.toSet()));
            assertEquals(ImmutableSet.of("doc1", "doc3"), tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.NOT_EQUAL, Instant.ofEpochSecond(2)))).collect(Collectors.toSet()));


            //Update some data
            add(store, "doc4", getDocument("It's all a big Bob", -100, 11.2, Geoshape.point(-48.0, 8.0), Geoshape.point(-48.0, 8.0), Arrays.asList("10", "11", "12"), Sets.newHashSet("10", "11"), Instant.ofEpochSecond(4),
                false), true);
            remove(store, "doc2", doc2, true);
            remove(store, "doc3", ImmutableMultimap.of(WEIGHT, 10.1), false);
            add(store, "doc3", ImmutableMultimap.of(TIME, 2000, TEXT, "Bob owns the world"), false);
            remove(store, "doc1", ImmutableMultimap.of(TIME, 1001), false);
            add(store, "doc1", ImmutableMultimap.of(TIME, 1005, WEIGHT, 11.1, LOCATION, Geoshape.point(-48.0, 0.0), BOUNDARY, Geoshape.circle(-48.0, 0.0, 1.0)), false);
            final Geoshape multiPoint = Geoshape.geoshape(Geoshape.getShapeFactory().multiPoint().pointXY(60.0, 60.0).pointXY(120.0, 60.0).build());
            add(store, "doc5", getDocument("A Full Yes", -100, -11.2, Geoshape.point(48.0, 8.0), multiPoint, Arrays.asList("10", "11", "12"), Sets.newHashSet("10", "11"), Instant.ofEpochSecond(400),
                false), true);
            final Geoshape multiLine = Geoshape.geoshape(Geoshape.getShapeFactory().multiLineString().add(Geoshape.getShapeFactory().lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0))
                .add(Geoshape.getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build());
            add(store, "doc6", getDocument("A Full Yes", -100, -11.2, Geoshape.point(48.0, 8.0), multiLine, Arrays.asList("10", "11", "12"), Sets.newHashSet("10", "11"), Instant.ofEpochSecond(400),
                false), true);
            final Geoshape multiPolygon = Geoshape.geoshape(Geoshape.getShapeFactory().multiPolygon()
                .add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0).pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
                .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0).pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build());
            add(store, "doc7", getDocument("A Full Yes", -100, -11.2, Geoshape.point(48.0, 8.0), multiPolygon, Arrays.asList("10", "11", "12"), Sets.newHashSet("10", "11"), Instant.ofEpochSecond(400),
                false), true);
            final Geoshape geometryCollection = Geoshape.geoshape(Geoshape.getGeometryCollectionBuilder().add(Geoshape.getShapeFactory().pointXY(60.0, 60.0))
                .add(Geoshape.getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build()).build());
            add(store, "doc8", getDocument("A Full Yes", -100, -11.2, Geoshape.point(48.0, 8.0), geometryCollection, Arrays.asList("10", "11", "12"), Sets.newHashSet("10", "11"), Instant.ofEpochSecond(400),
                false), true);
        }

        clopen();

        for (final String store : stores) {

            List<String> result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")))
                    .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc3"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"), PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 6.0))))
                    .collect(Collectors.toList());
            assertEquals(1, result.size());
            assertEquals("doc1", result.get(0));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00))))
                    .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "tomorrow"), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00)))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of(), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "tomorrow"), PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00)))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of(), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of(TIME, Cmp.LESS_THAN, 1010), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 1000.00)))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc4"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, -1000), PredicateCondition.of(TIME, Cmp.LESS_THAN, 1010), PredicateCondition.of(BOUNDARY, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 1000.00)))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc4"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 10.0))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc1", "doc4"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of("blah", Cmp.GREATER_THAN, 10.0))))
                .collect(Collectors.toList());
            assertEquals(0, result.size());

            if (index.supports(new StandardKeyInformation(String.class, Cardinality.LIST, new Parameter<>("mapping", Mapping.STRING)), Cmp.EQUAL)) {
                for (int suffix=4; suffix<=8; suffix++) {
                    final String suffixString = "doc" + suffix;
                    assertTrue(tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "10"))).anyMatch(suffixString::equals));
                    assertTrue(tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "11"))).anyMatch(suffixString::equals));
                    assertTrue(tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "10"))).anyMatch(suffixString::equals));
                    assertTrue(tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "11"))).anyMatch(suffixString::equals));
                }
                assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "4"))).count());
                assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "5"))).count());
            }

            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(2)))).count());
            assertEquals("doc4", tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(4)))).findFirst().get());

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.INTERSECT, Geoshape.circle(59, 59, 200.00))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc5", "doc6","doc7", "doc8"), Sets.newHashSet(result));

            result = tx.queryStream(new IndexQuery(store, PredicateCondition.of(BOUNDARY, Geo.INTERSECT, Geoshape.circle(59, 119, 200.00))))
                .collect(Collectors.toList());
            assertEquals(ImmutableSet.of("doc5", "doc6", "doc7", "doc8"), Sets.newHashSet(result));
        }

    }

    @Test
    public void testCommonSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE)));

        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.STRING))));

        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.STRING)), Cmp.GREATER_THAN));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.STRING)), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.STRING)), Cmp.LESS_THAN));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.STRING)), Cmp.LESS_THAN_EQUAL));

        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXT)), Cmp.GREATER_THAN));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXT)), Cmp.GREATER_THAN_EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXT)), Cmp.LESS_THAN_EQUAL));

        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.DEFAULT)), Cmp.GREATER_THAN));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.DEFAULT)), Cmp.GREATER_THAN_EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.DEFAULT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.DEFAULT)), Cmp.LESS_THAN_EQUAL));

        if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) {
            assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXTSTRING)), Cmp.GREATER_THAN));
            assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXTSTRING)), Cmp.GREATER_THAN_EQUAL));
            assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXTSTRING)), Cmp.LESS_THAN));
            assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>("mapping", Mapping.TEXTSTRING)), Cmp.LESS_THAN_EQUAL));
        }

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.TEXT))));

        assertTrue(index.supports(of(Long.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Long.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.DEFAULT))));
        assertTrue(index.supports(of(Integer.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Short.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Byte.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Float.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Object.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Exception.class, Cardinality.SINGLE)));

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.GREATER_THAN));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.LESS_THAN));

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.LESS_THAN_EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.DEFAULT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.TEXT)), Cmp.LESS_THAN));


        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE), Geo.INTERSECT));
        assertFalse(index.supports(of(Long.class, Cardinality.SINGLE), Text.CONTAINS));

        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.INTERSECT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.PREFIX_TREE)), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.PREFIX_TREE)), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>("mapping",Mapping.PREFIX_TREE)), Geo.INTERSECT));
    }

    @Test
    public void largeTest() throws Exception {
        final int numDoc = 30000;
        final String store = "vertex";
        initialize(store);
        for (int i = 1; i <= numDoc; i++) {
            add(store, "doc" + i, getRandomDocument(), true);
        }
        clopen();

        final long time = System.currentTimeMillis();
        Stream<String> result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of(WEIGHT, Cmp.LESS_THAN, 0.6), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00)))));
        final long oldResultSize = result.count();
        System.out.println(oldResultSize + " vs " + (numDoc / 1000 * 2.4622623015));
        System.out.println("Query time on " + numDoc + " docs (ms): " + (System.currentTimeMillis() - time));
        result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of(WEIGHT, Cmp.LESS_THAN, 0.6), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))), numDoc / 1000));
        assertEquals(numDoc / 1000, result.count());
        result = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.2), PredicateCondition.of(WEIGHT, Cmp.LESS_THAN, 0.6), PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(48.5, 0.5, 1000.00))), numDoc / 1000 * 100));
        assertEquals(oldResultSize, result.count());
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
        Set<String> results = tx.queryStream(new IndexQuery(store1, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(2, results.size());

        // now let's try to restore (change values on the existing doc2, delete doc1, and add a new doc)
        index.restore(new HashMap<String, Map<String, List<IndexEntry>>>() {{
            put(store1, new HashMap<String, List<IndexEntry>>() {{
                put("restore-doc1", Collections.emptyList());
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
        results = tx.queryStream(new IndexQuery(store1, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc3"));

        // check if the name and time was set correctly for doc3
        results = tx.queryStream(new IndexQuery(store1, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "third"), PredicateCondition.of(TIME, Cmp.EQUAL, 3L))))
                .collect(Collectors.toSet());
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc3"));

        // let's check if all of the new properties where set correctly from doc2
        results = tx.queryStream(new IndexQuery(store1, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "not-second"), PredicateCondition.of(TIME, Cmp.EQUAL, 0L))))
                .collect(Collectors.toSet());
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
        results = tx.queryStream(new IndexQuery(store1, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(2, results.size());
        assertTrue(results.contains("restore-doc1"));
        assertTrue(results.contains("restore-doc3"));

        // check if the name and time was set correctly for doc1
        results = tx.queryStream(new IndexQuery(store1, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "first-restored"), PredicateCondition.of(TIME, Cmp.EQUAL, 4L))))
                .collect(Collectors.toSet());
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc1"));

        // now let's check second store and see if we got doc1 added there too
        results = tx.queryStream(new IndexQuery(store2, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc1"));

        // check if the name and time was set correctly for doc1 (in second store)
        results = tx.queryStream(new IndexQuery(store2, And.of(PredicateCondition.of(NAME, Cmp.EQUAL, "first-in-second-store"), PredicateCondition.of(TIME, Cmp.EQUAL, 5L))))
                .collect(Collectors.toSet());
        assertEquals(1, results.size());
        assertTrue(results.contains("restore-doc1"));
    }

    @RepeatedIfExceptionsTest(repeats = 4, minSuccess = 2)
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
        Set<String> results = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(4, results.size());

        Thread.sleep(6000); // sleep for elastic search ttl recycle

        results = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(2, results.size());
        assertTrue(results.contains("expiring-doc2"));
        assertTrue(results.contains("expiring-doc4"));

        Thread.sleep(5000); // sleep for elastic search ttl recycle
        results = tx.queryStream(new IndexQuery(store, And.of(PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 4.0))))
                .collect(Collectors.toSet());
        assertEquals(1, results.size());
        assertTrue(results.contains("expiring-doc2"));
    }

   /* ==================================================================================
                            CONCURRENT UPDATE CASES
     ==================================================================================*/


    private final String defStore = "store1";
    private final String defDoc = "docx1-id";
    private final String defTextValue = "the quick brown fox jumps over the lazy dog";

    private interface TxJob {
        void run(IndexTransaction tx);
    }

    private void runConflictingTx(TxJob job1, TxJob job2) throws Exception {
        initialize(defStore);
        final Multimap<String, Object> initialProps = ImmutableMultimap.of(TEXT, defTextValue);
        add(defStore, defDoc, initialProps, true);
        clopen();

        // Sanity check
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),defDoc);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "periwinkle")),null);

        final IndexTransaction tx1 = openTx(), tx2 = openTx();
        job1.run(tx1);
        tx1.commit();
        job2.run(tx2);
        tx2.commit();

        clopen();
    }

    private void checkResult(IndexQuery query, String containedDoc) throws Exception {
        final List<String> result = tx.queryStream(query).collect(Collectors.toList());
        if (containedDoc!=null) {
            assertEquals(1, result.size());
            assertEquals(containedDoc, result.get(0));
        } else {
            assertEquals(0, result.size());
        }
    }


    @Test
    public void testDeleteDocumentThenDeleteField() throws Exception {
        runConflictingTx(tx -> tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true),
            tx -> tx.delete(defStore, defDoc, TEXT, defTextValue, false));

        // Document must not exist
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
    }

    @Test
    public void testDeleteDocumentThenModifyField() throws Exception {
        runConflictingTx(tx -> tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true),
            tx -> tx.add(defStore, defDoc, TEXT, "the slow brown fox jumps over the lazy dog", false));

        //2nd tx should put document back into existence
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),defDoc);
    }

    @Test
    public void testDeleteDocumentThenAddField() throws Exception {
        final String nameValue = "jm keynes";

        runConflictingTx(tx -> tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true),
            tx -> tx.add(defStore, defDoc, NAME, nameValue, false));

        // TEXT field should have been deleted when document was
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
        // but name field should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(NAME, Cmp.EQUAL, nameValue)),defDoc);
    }

    @Test
    public void testAddFieldThenDeleteDoc() throws Exception {
        final String nameValue = "jm keynes";

        runConflictingTx(tx -> tx.add(defStore, defDoc, NAME, nameValue, false),
            tx -> tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), true));

        //neither should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(NAME, Cmp.EQUAL, nameValue)),null);
    }

    @Test
    public void testConflictingAdd() throws Exception {
        final String doc2 = "docy2";
        runConflictingTx(tx -> {
            final Multimap<String, Object> initialProps = ImmutableMultimap.of(TEXT, "sugar sugar");
            add(defStore, doc2, initialProps, true);
        }, tx -> {
            final Multimap<String, Object> initialProps = ImmutableMultimap.of(TEXT, "honey honey");
            add(defStore, doc2, initialProps, true);
        });

        //only last write should be visible
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),defDoc);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "sugar")),null);
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "honey")),doc2);
    }

    @Test
    public void testLastWriteWins() throws Exception {
        runConflictingTx(tx -> {
            tx.delete(defStore, defDoc, TEXT, defTextValue, false);
            tx.add(defStore, defDoc, TEXT, "sugar sugar", false);
        }, tx -> {
            tx.delete(defStore, defDoc, TEXT, defTextValue, false);
            tx.add(defStore, defDoc, TEXT, "honey honey", false);
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
        runConflictingTx(tx -> tx.add(defStore, defDoc, TEXT, revisedText, false), tx -> {/*do nothing*/});

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
        runConflictingTx(tx -> tx.delete(defStore, defDoc, TEXT, ImmutableMap.of(), false), tx -> {/*do nothing*/});

        // Should no longer return deleted text
        checkResult(new IndexQuery(defStore, PredicateCondition.of(TEXT, Text.CONTAINS, "brown")),null);
    }

    /**
     * Test custom analyzer
     * @throws Exception
     */
    @Test
    public void testCustomAnalyzer() throws Exception {
        if (!indexFeatures.supportsCustomAnalyzer())
            return;
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> initialDoc = HashMultimap.create();

        initialDoc.put(STRING, "Tom and Jerry");
        initialDoc.put(ANALYZED, "Tom and Jerry");
        if(indexFeatures.supportsStringMapping(Mapping.TEXTSTRING))
            initialDoc.put(FULL_TEXT, "Tom and Jerry");
        initialDoc.put(KEYWORD, "Tom and Jerry");
        add(store, "docId", initialDoc, true);
        clopen();

        IndexQuery query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.EQUAL, "Tom and Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.EQUAL, "Tom"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.NOT_EQUAL, "Tom"));
        assertEquals(0, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.NOT_EQUAL, "Tom Jerry"));
        assertEquals(0, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.EQUAL, "Tom Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.EQUAL, "Tom or Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Text.PREFIX, "jerr"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(STRING, Text.REGEX, "jer.*"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS, "Tom and Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS, "Tom Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS, "Tom"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS, "Tom or Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS_PREFIX, "jerr"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS_REGEX, "jer.*"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        if(indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)){
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.EQUAL, "Tom and Jerry"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.EQUAL, "Tom Jerry"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.EQUAL, "Tom or Jerry"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.PREFIX, "jerr"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.REGEX, "jer.*"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.CONTAINS, "Tom and Jerry"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.CONTAINS, "Tom Jerry"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.CONTAINS, "Tom or Jerry"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.CONTAINS_PREFIX, "jerr"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());
            query = new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Text.CONTAINS_REGEX, "jer.*"));
            assertEquals(1, tx.queryStream(query).count(), query.toString());

            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.GREATER_THAN, "a"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.GREATER_THAN, "z"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.GREATER_THAN, "Tom and Jerry"))).size());

            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.GREATER_THAN_EQUAL, "a"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.GREATER_THAN_EQUAL, "z"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.GREATER_THAN_EQUAL, "Tom and Jerry"))).size());

            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.LESS_THAN, "a"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.LESS_THAN, "z"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.LESS_THAN, "Tom and Jerry"))).size());

            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.LESS_THAN_EQUAL, "a"))).size());
            assertEquals(1, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.LESS_THAN_EQUAL, "z"))).size());
            assertEquals(0, tx.query(new IndexQuery(store, PredicateCondition.of(FULL_TEXT, Cmp.LESS_THAN_EQUAL, "Tom and Jerry"))).size());
        }
        query = new IndexQuery(store, PredicateCondition.of(KEYWORD, Text.CONTAINS_PREFIX, "Tom"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(KEYWORD, Text.CONTAINS_REGEX, ".*Jer.*"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());

    }

    @Test
    public void testScroll() throws BackendException {
        final String store = "vertex";

        initialize(store);
        add(store, "doc1", getDocument(1001,  5.2), true);
        add(store, "doc2", getDocument(1001,  5.2), true);
        add(store, "doc3", getDocument(1001,  6.2), true);
        add(store, "doc4", getDocument(1002,  7.2), true);
        add(store, "doc5", getDocument(1002,  8.2), true);
        add(store, "doc6", getDocument(1002,  9.2), true);
        add(store, "doc7", getDocument(1002, 10.2), true);

        clopen();

        //Test res < batchSize
        assertEquals(2, tx.queryStream(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.EQUAL, 5.2))).count());
        //Test res = batchSize
        assertEquals(3, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.EQUAL, 1001))).count());
        //Test res > batchSize
        assertEquals(7, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "Hello world"))).count());
        //Test res == limit
        assertEquals(4, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.EQUAL, 1002), 4)).count());
        //Test limit % batchSize == 0
        assertEquals(6, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "Hello world"), 6)).count());
    }

    @Test
    public void testPersistentIndexData() throws BackendException {
        final String store = "vertex";

        final Multimap<String, Object> initialDoc = ArrayListMultimap.create();
        initialDoc.put(DATE, Instant.ofEpochSecond(1));
        double latitude = 30;
        double longitude = 60;
        initialDoc.put(LOCATION, Geoshape.point(latitude, longitude));

        initialDoc.put(STRING, "network");
        initialDoc.put(ANALYZED, "network");
        initialDoc.put(KEYWORD, "network");
        initialDoc.put(NAME, "network");
        initialDoc.put(TIME, 1L);
        initialDoc.put(WEIGHT, 1.5d);

        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            initialDoc.put(PHONE_LIST, "one");
            initialDoc.put(PHONE_LIST, "two");
        }
        if (indexFeatures.supportsCardinality(Cardinality.SET)) {
            initialDoc.put(PHONE_SET, "three");
            initialDoc.put(PHONE_SET, "four");

            initialDoc.put(TIME_TICK, new Date(1));
            initialDoc.put(TIME_TICK, new Date(2));
            initialDoc.put(TIME_TICK, new Date(2));
            initialDoc.put(TIME_TICK, new Date(3));
        }

        initialize(store);
        add(store, "doc1", initialDoc, true);

        clopen();

        // update the document
        tx.add(store, "doc1", TEXT, "update", false);
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            tx.delete(store, "doc1", TIME_TICK, new Date(2), false);
            tx.delete(store, "doc1", TIME_TICK, new Date(3), false);
        }
        tx.commit();

        // check every indexed type is still in the index
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(DATE, Cmp.EQUAL, Instant.ofEpochSecond(1)))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.box(latitude-1L, longitude-1, latitude+1, longitude+1)))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.EQUAL, 1.5d))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.EQUAL, 1L))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(STRING, Cmp.EQUAL, "network"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(KEYWORD, Text.CONTAINS, "network"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(ANALYZED, Text.CONTAINS, "network"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(NAME, Cmp.EQUAL, "network"))).count());

        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "one"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "two"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME_TICK, Cmp.EQUAL, new Date(1)))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME_TICK, Cmp.EQUAL, new Date(2)))).count());
            assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME_TICK, Cmp.EQUAL, new Date(3)))).count());
        }
        if (indexFeatures.supportsCardinality(Cardinality.SET)) {
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "three"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "four"))).count());
        }
    }

    @Test
    public void clearStorageTest() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> doc1 = getDocument("Hello world", 1001, 5.2, Geoshape.point(48.0, 0.0), Geoshape.polygon(Arrays.asList(new double[][]{{-0.1, 47.9}, {0.1, 47.9}, {0.1, 48.1}, {-0.1, 48.1}, {-0.1, 47.9}})), Arrays.asList("1", "2", "3"), Sets.newHashSet("1", "2"), Instant.ofEpochSecond(1),
            false);
        add(store, "doc1", doc1, true);
        clopen();
        assertTrue(index.exists());
        tearDown();
        setUp();
        assertFalse(index.exists());
    }

    @ParameterizedTest
    @MethodSource("org.janusgraph.core.attribute.TextArgument#text")
    public void testTextPredicate(JanusGraphPredicate predicate, boolean expected, String value, String condition) throws BackendException {
        assumeIndexSupportFor(Mapping.TEXT, predicate);
        if (value != null)
            initializeWithDoc("vertex", "test1", TEXT, value, true);
        else
            // if the value is null, replicate a missing field by indexing a different one
            initializeWithDoc("vertex", "test1", BOOLEAN, true, true);
        testPredicateByCount((expected) ? 1 : 0, predicate, TEXT, condition);
    }

    @ParameterizedTest
    @MethodSource("org.janusgraph.core.attribute.TextArgument#string")
    public void testStringPredicate(JanusGraphPredicate predicate, boolean expected, String value, String condition) throws BackendException {
        assumeIndexSupportFor(Mapping.STRING, predicate);
        if (value != null)
            initializeWithDoc("vertex", "test1", NAME, value, true);
        else
            // if the value is null, replicate a missing field by indexing a different one
            initializeWithDoc("vertex", "test1", BOOLEAN, true, true);
        testPredicateByCount((expected) ? 1 : 0, predicate, NAME, condition);
    }

    /* ==================================================================================
                            HELPER METHODS
     ==================================================================================*/

    /**
     * Initialize the store, and add a test document with the provided
     * field/value pair.
     *
     * @param store
     * @param docId
     * @param field
     * @param value
     * @param isNew
     * @throws BackendException
     */
    private void initializeWithDoc(String store, String docId, String field, Object value, boolean isNew) throws BackendException {
        initialize(store);

        Multimap<String, Object> doc = HashMultimap.create();
        doc.put(field, value);

        add(store, docId, doc, isNew);

        clopen();
    }

    /**
     * Tests the index to ensure it supports the provided mapping,
     * and the provided predicate.
     *
     * @param mapping
     * @param predicate
     */
    protected void assumeIndexSupportFor(Mapping mapping, JanusGraphPredicate predicate) {
        Assume.assumeThat("Index supports mapping"+mapping, indexFeatures.supportsStringMapping(mapping), is(true));
        Assume.assumeThat("Index supports predicate "+predicate+" for mapping "+mapping, supportsPredicateFor(mapping, predicate), is(true));
    }

    protected boolean supportsPredicateFor(Mapping mapping, Class<?> dataType, Cardinality cardinality, JanusGraphPredicate predicate) {
        return index.supports(new StandardKeyInformation(dataType, cardinality, mapping.asParameter()), predicate);
    }

    protected boolean supportsPredicateFor(Mapping mapping, Class<?> dataType, JanusGraphPredicate predicate) {
        return supportsPredicateFor(mapping, dataType, Cardinality.SINGLE, predicate);
    }

    protected boolean supportsPredicateFor(Mapping mapping, JanusGraphPredicate predicate) {
        return supportsPredicateFor(mapping, String.class, Cardinality.SINGLE, predicate);
    }

    protected long getDocCountByPredicate(JanusGraphPredicate predicate, String field, String condition) throws BackendException {
        return tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(field, predicate, condition))).count();
    }

    private void testPredicateByCount(long expectation, JanusGraphPredicate predicate, String field, String condition) throws BackendException {
        assertEquals(expectation, getDocCountByPredicate(predicate, field, condition));
    }

    protected void initialize(String store) throws BackendException {
        for (final Map.Entry<String,KeyInformation> info : allKeys.entrySet()) {
            final KeyInformation keyInfo = info.getValue();
            if (index.supports(keyInfo)) index.register(store,info.getKey(),keyInfo,tx);
        }
    }

    protected void add(String store, String documentId, Multimap<String, Object> doc, boolean isNew) {
        add(store, documentId, doc, isNew, 0);
    }

    private void add(String store, String documentId, Multimap<String, Object> doc, boolean isNew, int ttlInSeconds) {
        for (final Map.Entry<String, Object> kv : doc.entries()) {
            if (!index.supports(allKeys.get(kv.getKey())))
                continue;

            final IndexEntry idx = new IndexEntry(kv.getKey(), kv.getValue());
            if (ttlInSeconds > 0)
                idx.setMetaData(EntryMetaData.TTL, ttlInSeconds);

            tx.add(store, documentId, idx, isNew);
        }
    }

    private void remove(String store, String documentId, Multimap<String, Object> doc, boolean deleteAll) {
        for (final Map.Entry<String, Object> kv : doc.entries()) {
            if (index.supports(allKeys.get(kv.getKey()))) {
                tx.delete(store, documentId, kv.getKey(), kv.getValue(), deleteAll);
            }
        }
    }


    public Multimap<String, Object> getDocument(final String txt, final long time, final double weight, final Geoshape location,
                                                final Geoshape boundary, List<String> phoneList, Set<String> phoneSet, Instant date,
                                                final Boolean bool) {
        final HashMultimap<String, Object> values = HashMultimap.create();
        values.put(TEXT, txt);
        values.put(NAME, txt);
        values.put(TIME, time);
        values.put(WEIGHT, weight);
        values.put(LOCATION, location);
        values.put(BOUNDARY, boundary);
        values.put(DATE, date);
        values.put(BOOLEAN, bool);
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            for (final String phone : phoneList) {
                values.put(PHONE_LIST, phone);
            }
        }
        if(indexFeatures.supportsCardinality(Cardinality.SET)) {
            for (final String phone : phoneSet) {
                values.put(PHONE_SET, phone);
            }
        }
        return values;
    }

    public static Multimap<String, Object> getRandomDocument() {
        final StringBuilder s = new StringBuilder();
        for (int i = 0; i < 3; i++) s.append(RandomGenerator.randomString(5, 8)).append(" ");
        final Multimap<String, Object> values = HashMultimap.create();

        values.put(TEXT, s.toString());
        values.put(NAME, s.toString());
        values.put(TIME, Math.abs(random.nextLong()));
        values.put(WEIGHT, random.nextDouble());
        values.put(LOCATION, Geoshape.point(random.nextDouble() * 180 - 90, random.nextDouble() * 360 - 180));
        return values;
    }

    public static void printResult(Iterable<RawQuery.Result<String>> result) {
        for (final RawQuery.Result<String> r : result) {
            System.out.println(r.getResult() + ":"+r.getScore());
        }
    }

    private Multimap<String, Object> getDocument(final long time, final double weight) {
        final Multimap<String, Object> toReturn = HashMultimap.create();
        toReturn.put(NAME, "Hello world");
        toReturn.put(TIME, time);
        toReturn.put(WEIGHT, weight);
        return toReturn;
    }

}
