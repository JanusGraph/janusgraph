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

package org.janusgraph.diskstorage.lucene;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LuceneIndexTest extends IndexProviderTest {

    private static final Logger log =
            LoggerFactory.getLogger(LuceneIndexTest.class);

    private static char REPLACEMENT_CHAR = '\u2022';
    private static final String MAPPING = "mapping";
    private static final String ALIASES = "aliases", NAMES = "names", TAGS = "tags", FAILING = "failing";

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new LuceneIndex(getLocalLuceneTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public String getEnglishAnalyzerName() {
        return org.apache.lucene.analysis.en.EnglishAnalyzer.class.getName();
    }
    
    @Override
    public String getKeywordAnalyzerName() {
        return org.apache.lucene.analysis.core.KeywordAnalyzer.class.getName();
    }

    @Override
    public Mapping preferredGeoShapeMapping() {
        return Mapping.PREFIX_TREE;
    }

    public static Configuration getLocalLuceneTestConfig() {
        final String index = "lucene";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"),index);
        return config.restrictTo(index);
    }

    @Test
    public void testOrderByStringWithCustomAnalyzerOverManyDocuments() throws Exception {
        // Once more documents than max(limit, 1000) match, Lucene skips the non-competitive documents of a sort on
        // doc values through the terms index of the field. The terms of STRING and of the string part of FULL_TEXT
        // are made by a custom string analyzer and differ from their doc values. TEXT_STRING has no custom string
        // analyzer, so its sort keeps skipping documents.
        final String store = "store1";
        initialize(store);
        final int numDocs = 1500;
        for (int i = 0; i < numDocs; i++) {
            final Multimap<String, Object> doc = HashMultimap.create();
            doc.put(STRING, String.format("Walking person %04d", numDocs - i));
            doc.put(FULL_TEXT, String.format("Walking person %04d", numDocs - i));
            doc.put(TEXT_STRING, String.format("Walking person %04d", numDocs - i));
            doc.put(TIME, (long) i);
            add(store, "doc" + i, doc, true);
        }
        clopen();

        final List<String> expected = IntStream.range(0, 10).mapToObj(i -> "doc" + (numDocs - 1 - i)).collect(Collectors.toList());
        for (String key : new String[]{STRING, FULL_TEXT, TEXT_STRING}) {
            final List<String> docIds = tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, 0L),
                ImmutableList.of(new IndexQuery.OrderEntry(key, Order.ASC, String.class)), 10)).collect(Collectors.toList());
            assertEquals(expected, docIds, key);
        }
    }

    @Test
    public void testSupport() {
        // DEFAULT(=TEXT) support
        assertTrue( index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue( index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS_PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS_REGEX)); // TODO Not supported yet
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        // Same tests as above, except explicitly specifying TEXT instead of relying on DEFAULT
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS_FUZZY));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS_REGEX)); // TODO Not supported yet
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Cmp.EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Cmp.NOT_EQUAL));

        // STRING support
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.CONTAINS));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.LESS_THAN_EQUAL));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.GREATER_THAN));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Boolean.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Boolean.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(UUID.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(UUID.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.INTERSECT));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.DISJOINT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.INTERSECT));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.DISJOINT));
    }


    @Test
    public void testMapKey2Field_IllegalCharacter() {
        assertThrows(IllegalArgumentException.class, () ->{
            index.mapKey2Field("here is an illegal character: " + REPLACEMENT_CHAR, null);
        });
    }

    @Test
    public void testMapKey2Field_MappingSpaces() {
        String expected = "field" + REPLACEMENT_CHAR + "name" + REPLACEMENT_CHAR + "with" + REPLACEMENT_CHAR + "spaces";
        assertEquals(expected, index.mapKey2Field("field name with spaces", null));
    }

    // Decimals at the infinities, which Lucene stores and Elasticsearch doesn't: a strict bound at an infinity matches
    // nothing, an inclusive one the infinity itself, and the other comparisons reach them
    @Test
    public void rangeQueriesReachTheInfinities() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Object[][] values = {{"minus", Double.NEGATIVE_INFINITY}, {"zero", 0.0}, {"plus", Double.POSITIVE_INFINITY}};
        for (final Object[] value : values) {
            final Multimap<String, Object> doc = HashMultimap.create();
            doc.put(WEIGHT, value[1]);
            add(store, (String) value[0], doc, true);
        }
        clopen();
        assertEquals(ImmutableSet.of(), rangeQuery(store, WEIGHT, Cmp.LESS_THAN, Double.NEGATIVE_INFINITY));
        assertEquals(ImmutableSet.of("minus"), rangeQuery(store, WEIGHT, Cmp.LESS_THAN_EQUAL, Double.NEGATIVE_INFINITY));
        assertEquals(ImmutableSet.of(), rangeQuery(store, WEIGHT, Cmp.GREATER_THAN, Double.POSITIVE_INFINITY));
        assertEquals(ImmutableSet.of("plus"), rangeQuery(store, WEIGHT, Cmp.GREATER_THAN_EQUAL, Double.POSITIVE_INFINITY));
        assertEquals(ImmutableSet.of("minus", "zero"), rangeQuery(store, WEIGHT, Cmp.NOT_EQUAL, Double.POSITIVE_INFINITY));
        assertEquals(ImmutableSet.of("zero", "plus"), rangeQuery(store, WEIGHT, Cmp.NOT_EQUAL, Double.NEGATIVE_INFINITY));
        assertEquals(ImmutableSet.of("plus"), rangeQuery(store, WEIGHT, Cmp.GREATER_THAN, 0.0));
        assertEquals(ImmutableSet.of("minus"), rangeQuery(store, WEIGHT, Cmp.LESS_THAN, 0.0));
    }

    // A decimal comparison finds what its evaluation in memory, Cmp.test, finds. Lucene orders -0.0 below 0.0, which
    // a comparison holds equal and equality alone tells apart, and NaN above positive infinity, while NaN equals NaN
    // alone and compares to no other value
    @Test
    public void decimalComparisonsFindWhatTheInMemoryPredicatesFind() throws Exception {
        final String store = "vertex";
        initialize(store);
        final String[] names = {"minusInfinity", "minusOne", "belowZero", "minusZero", "plusZero", "aboveZero", "one",
            "plusInfinity", "nan"};
        final double[] values = {Double.NEGATIVE_INFINITY, -1.0, -Double.MIN_VALUE, -0.0, 0.0, Double.MIN_VALUE, 1.0,
            Double.POSITIVE_INFINITY, Double.NaN};
        for (int i = 0; i < names.length; i++) {
            final Multimap<String, Object> doc = HashMultimap.create();
            doc.put(WEIGHT, values[i]);
            add(store, names[i], doc, true);
        }
        clopen();
        for (final double bound : values) {
            for (final Cmp relation : Cmp.values()) {
                final Set<String> inMemory = IntStream.range(0, names.length)
                    .filter(i -> relation.test(values[i], bound)).mapToObj(i -> names[i]).collect(Collectors.toSet());
                assertEquals(inMemory, rangeQuery(store, WEIGHT, relation, bound), relation + " " + bound);
            }
        }
        //Some of the expectations above
        assertEquals(ImmutableSet.of("nan"), rangeQuery(store, WEIGHT, Cmp.EQUAL, Double.NaN));
        assertEquals(ImmutableSet.of("nan"), rangeQuery(store, WEIGHT, Cmp.LESS_THAN_EQUAL, Double.NaN));
        assertEquals(ImmutableSet.of(), rangeQuery(store, WEIGHT, Cmp.GREATER_THAN, Double.NaN));
        assertFalse(rangeQuery(store, WEIGHT, Cmp.NOT_EQUAL, Double.NaN).contains("nan"));
        assertTrue(rangeQuery(store, WEIGHT, Cmp.NOT_EQUAL, 1.0).contains("nan"));
        assertEquals(ImmutableSet.of("minusZero"), rangeQuery(store, WEIGHT, Cmp.EQUAL, -0.0));
        assertEquals(ImmutableSet.of("minusInfinity", "minusOne", "belowZero"),
            rangeQuery(store, WEIGHT, Cmp.LESS_THAN, 0.0));
    }

    // A raw range orders decimals as Lucene does: NaN above positive infinity, as the value next to it, with nothing
    // beyond NaN, which the range reaches by a NaN bound alone
    @Test
    public void rawRangeQueriesOrderNaNAbovePositiveInfinity() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Object[][] values = {{"one", 1.0}, {"plusInfinity", Double.POSITIVE_INFINITY}, {"nan", Double.NaN}};
        for (final Object[] value : values) {
            final Multimap<String, Object> doc = HashMultimap.create();
            doc.put(WEIGHT, value[1]);
            add(store, (String) value[0], doc, true);
        }
        clopen();
        assertEquals(ImmutableSet.of("nan"), rawQuery(store, WEIGHT + ":[NaN TO NaN]"));
        assertEquals(ImmutableSet.of("plusInfinity", "nan"), rawQuery(store, WEIGHT + ":[5 TO NaN]"));
        assertEquals(ImmutableSet.of("nan"), rawQuery(store, WEIGHT + ":{Infinity TO NaN]"));
        assertEquals(ImmutableSet.of("one", "plusInfinity", "nan"), rawQuery(store, WEIGHT + ":[* TO NaN]"));
        assertEquals(ImmutableSet.of("one", "plusInfinity"), rawQuery(store, WEIGHT + ":{* TO NaN}"));
        assertEquals(ImmutableSet.of(), rawQuery(store, WEIGHT + ":{NaN TO *]"));
        assertEquals(ImmutableSet.of(), rawQuery(store, WEIGHT + ":[NaN TO 5]"));
        assertEquals(ImmutableSet.of(), rawQuery(store, WEIGHT + ":{Infinity TO *]"));
    }

    // Exclusive bounds with nothing between them, as {5 TO 5] or [0.0 TO 0.0}, match nothing rather than failing
    @Test
    public void rawRangeQueriesWithNothingBetweenTheirBoundsMatchNothing() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> doc = HashMultimap.create();
        doc.put(TIME, 5L);
        doc.put(WEIGHT, 0.0);
        add(store, "five", doc, true);
        clopen();
        assertEquals(ImmutableSet.of("five"), rawQuery(store, TIME + ":[5 TO 5]"));
        assertEquals(ImmutableSet.of(), rawQuery(store, TIME + ":{5 TO 5]"));
        assertEquals(ImmutableSet.of(), rawQuery(store, TIME + ":[5 TO 5}"));
        assertEquals(ImmutableSet.of("five"), rawQuery(store, WEIGHT + ":[0.0 TO 0.0]"));
        assertEquals(ImmutableSet.of(), rawQuery(store, WEIGHT + ":{0.0 TO 0.0]"));
    }

    // Raw range queries: an open end (*) stays at the end of its type in an exclusive bracket too, and a strict bound at
    // the end of its type matches nothing rather than overflowing
    @Test
    public void rawRangeQueriesReachTheEndsOfTheirType() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Object[][] values = {{"min", Long.MIN_VALUE, Double.NEGATIVE_INFINITY}, {"zero", 0L, 0.0},
            {"max", Long.MAX_VALUE, Double.POSITIVE_INFINITY}};
        for (final Object[] value : values) {
            final Multimap<String, Object> doc = HashMultimap.create();
            doc.put(TIME, value[1]);
            doc.put(WEIGHT, value[2]);
            add(store, (String) value[0], doc, true);
        }
        clopen();
        assertEquals(ImmutableSet.of(), rawQuery(store, "time:{" + Long.MAX_VALUE + " TO *]"));
        assertEquals(ImmutableSet.of(), rawQuery(store, "time:[* TO " + Long.MIN_VALUE + "}"));
        assertEquals(ImmutableSet.of("zero", "max"), rawQuery(store, "time:{" + Long.MIN_VALUE + " TO *}"));
        assertEquals(ImmutableSet.of("min", "zero"), rawQuery(store, "time:{* TO " + Long.MAX_VALUE + "}"));
        assertEquals(ImmutableSet.of("zero", "max"), rawQuery(store, "weight:{-1 TO *}"));
        assertEquals(ImmutableSet.of("min", "zero"), rawQuery(store, "weight:{* TO 1}"));
    }

    // A key with a custom string analyzer equals a value when it holds every term the analyzer gives for the value, as
    // on Elasticsearch, and a not-equal finds the other documents with a value: those which lack one of the terms or
    // more, rather than those which lack them all
    @Test
    public void notEqualOfAnAnalyzedStringFindsTheDocumentsWithAValueWhichEqualityDoesNot() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> tomAndJerry = HashMultimap.create();
        tomAndJerry.put(STRING, "Tom and Jerry");
        tomAndJerry.put(FULL_TEXT, "Tom and Jerry");
        add(store, "tomAndJerry", tomAndJerry, true);
        final Multimap<String, Object> spike = HashMultimap.create();
        spike.put(STRING, "Spike");
        spike.put(FULL_TEXT, "Spike");
        add(store, "spike", spike, true);
        final Multimap<String, Object> withoutValue = HashMultimap.create();
        withoutValue.put(TIME, 1L);
        add(store, "withoutValue", withoutValue, true);
        clopen();

        for (final String key : new String[]{STRING, FULL_TEXT}) {
            assertEquals(ImmutableSet.of("tomAndJerry"), rangeQuery(store, key, Cmp.EQUAL, "Jerry Tom"), key);
            assertEquals(ImmutableSet.of("spike"), rangeQuery(store, key, Cmp.NOT_EQUAL, "Jerry Tom"), key);
            assertEquals(ImmutableSet.of(), rangeQuery(store, key, Cmp.EQUAL, "Tom Spike"), key);
            assertEquals(ImmutableSet.of("tomAndJerry", "spike"), rangeQuery(store, key, Cmp.NOT_EQUAL, "Tom Spike"),
                key);
            for (final String value : new String[]{"Tom and Jerry", "Jerry Tom", "Tom Spike", "Tom", "Spike", "the"}) {
                final Set<String> equal = rangeQuery(store, key, Cmp.EQUAL, value);
                assertEquals(Sets.difference(ImmutableSet.of("tomAndJerry", "spike"), equal),
                    rangeQuery(store, key, Cmp.NOT_EQUAL, value), key + " " + value);
            }
        }
    }

    // A value which the analyzer gives no term for, such as a stop word or punctuation, equals no document and is
    // contained in none, as on Elasticsearch: eq and textContains find and count nothing, and neq finds every document
    // with a value
    @Test
    public void analyzedValueWithoutTermsFindsNoDocument() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> doc = HashMultimap.create();
        doc.put(STRING, "Tom and Jerry");
        doc.put(TEXT, "Tom and Jerry");
        doc.put(ANALYZED, "Tom and Jerry");
        add(store, "tomAndJerry", doc, true);
        final Multimap<String, Object> withoutValue = HashMultimap.create();
        withoutValue.put(TIME, 1L);
        add(store, "withoutValue", withoutValue, true);
        clopen();

        assertEquals(ImmutableSet.of(), rangeQuery(store, STRING, Cmp.EQUAL, "the"));
        assertEquals(ImmutableSet.of(), rangeQuery(store, STRING, Cmp.EQUAL, "!!!"));
        assertEquals(ImmutableSet.of("tomAndJerry"), rangeQuery(store, STRING, Cmp.NOT_EQUAL, "the"));
        assertEquals(ImmutableSet.of(), textQuery(store, ANALYZED, Text.CONTAINS, "the"));
        assertEquals(ImmutableSet.of(), textQuery(store, TEXT, Text.CONTAINS, "!!!"));
        assertEquals(0L, tx.queryAggregation(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "!!!")),
            Aggregation.COUNT));
        assertEquals(1L, tx.queryAggregation(new IndexQuery(store, PredicateCondition.of(STRING, Cmp.NOT_EQUAL, "the")),
            Aggregation.COUNT));
    }

    private Set<String> textQuery(String store, String key, Text predicate, String value) throws BackendException {
        return tx.queryStream(new IndexQuery(store, PredicateCondition.of(key, predicate, value)))
            .collect(Collectors.toSet());
    }

    // The field of the whole strings of a TEXTSTRING key tells whether the document holds a value which a SET key gets
    // again without a deletion of it in the same mutation: when an addition is applied twice, or after a restore, as by
    // a reindex, which wrote the value already. The value's words then replace their entry, so removing the value
    // removes its words
    @Test
    public void setValueOfADualMappedKeyAddedAgainIsHeldOnce() throws Exception {
        final String store = "vertex";
        final KeyInformation.IndexRetriever retriever = getIndexRetriever(Collections.singletonMap(ALIASES,
            of(String.class, Cardinality.SET, Mapping.TEXTSTRING.asParameter())));
        for (final boolean isNew : new boolean[]{true, false}) {
            final IndexTransaction addition = openTx(retriever);
            addition.add(store, "addedTwice", ALIASES, "Zed", isNew);
            addition.commit();
        }
        final IndexTransaction restore = openTx(retriever);
        restore.restore(Collections.singletonMap(store, Collections.singletonMap("restored",
            Arrays.asList(new IndexEntry(ALIASES, "abc"), new IndexEntry(ALIASES, "Zed")))));
        restore.commit();
        final IndexTransaction addition = openTx(retriever);
        addition.add(store, "restored", ALIASES, "Zed", false);
        addition.commit();

        final IndexTransaction removal = openTx(retriever);
        removal.delete(store, "addedTwice", ALIASES, "Zed", false);
        removal.delete(store, "restored", ALIASES, "Zed", false);
        removal.commit();
        final IndexTransaction query = openTx(retriever);
        assertEquals(ImmutableSet.of(), query(query, store, ALIASES, Text.CONTAINS, "zed"));
        assertEquals(ImmutableSet.of(), query(query, store, ALIASES, Cmp.EQUAL, "Zed"));
        assertEquals(ImmutableSet.of("restored"), query(query, store, ALIASES, Text.CONTAINS, "abc"));
        query.commit();
    }

    // BackendOperation reattempts a mutation which failed in one store with the same map, after another store
    // committed. That store is taken out of the map: applied again, it would add the values of its LIST and SET keys
    // once more, whose words would then stay after the values are removed
    @Test
    public void reattemptedMutationLeavesOutTheStoresWhichCommitted() throws Exception {
        final KeyInformation.IndexRetriever retriever = getIndexRetriever(reattemptKeys());
        final IndexMutation committed = new IndexMutation(retriever.get("committed"), true, false);
        committed.addition(new IndexEntry(TAGS, "zed"));
        committed.addition(new IndexEntry(ALIASES, "zed"));
        final IndexMutation failing = new IndexMutation(retriever.get("failing"), true, false);
        failing.addition(new IndexEntry(FAILING, "fails once"));
        final Map<String, Map<String, IndexMutation>> mutations = new LinkedHashMap<>();
        mutations.put("committed", new HashMap<>(Collections.singletonMap("doc", committed)));
        mutations.put("failing", new HashMap<>(Collections.singletonMap("doc", failing)));

        final BaseTransactionConfigurable providerTx = index.beginTransaction(
            StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        FailingOnceAnalyzer.FAIL.set(true);
        try {
            assertThrows(TemporaryBackendException.class, () -> index.mutate(mutations, retriever, providerTx));
        } finally {
            FailingOnceAnalyzer.FAIL.set(false);
        }
        assertEquals(Collections.singleton("failing"), mutations.keySet());
        index.mutate(mutations, retriever, providerTx);
        providerTx.commit();

        final IndexTransaction removal = openTx(retriever);
        removal.delete("committed", "doc", TAGS, "zed", false);
        removal.delete("committed", "doc", ALIASES, "zed", false);
        removal.commit();
        final IndexTransaction query = openTx(retriever);
        assertEquals(ImmutableSet.of(), query(query, "committed", TAGS, Text.CONTAINS, "zed"));
        assertEquals(ImmutableSet.of(), query(query, "committed", ALIASES, Text.CONTAINS, "zed"));
        assertEquals(ImmutableSet.of("doc"), query(query, "failing", FAILING, Text.CONTAINS, "fails"));
        query.commit();

        //A map which the caller doesn't allow to change is left as it is, to be applied whole again
        final Map<String, Map<String, IndexMutation>> unmodifiable = Collections.unmodifiableMap(mutations);
        LuceneIndex.retainUncommittedStores(unmodifiable, Collections.singleton("failing"));
        assertEquals(Collections.singleton("failing"), unmodifiable.keySet());
    }

    // A store which fails before it commits rolls its writer back, so the reattempt, which applies the whole store
    // again, reads the documents as they were before the failed attempt. Each value is then added and removed once:
    // the values of a LIST key, and the values of SET keys whose words a case variant shares
    @Test
    public void reattemptedStoreAppliesItsChangesOnce() throws Exception {
        final String store = "vertex";
        final KeyInformation.IndexRetriever retriever = getIndexRetriever(reattemptKeys());
        final IndexTransaction setup = openTx(retriever);
        for (final String value : new String[]{"zed", "Zed"}) {
            setup.add(store, "doc", ALIASES, value, true);
            setup.add(store, "doc", NAMES, value, true);
            setup.add(store, "doc", TAGS, "tag", true);
        }
        setup.commit();

        final IndexMutation changes = new IndexMutation(retriever.get(store), false, false);
        changes.deletion(new IndexEntry(ALIASES, "zed"));
        changes.deletion(new IndexEntry(NAMES, "zed"));
        changes.deletion(new IndexEntry(TAGS, "tag"));
        changes.addition(new IndexEntry(ALIASES, "new"));
        changes.addition(new IndexEntry(NAMES, "New"));
        changes.addition(new IndexEntry(TAGS, "new"));
        final IndexMutation failing = new IndexMutation(retriever.get(store), true, false);
        failing.addition(new IndexEntry(FAILING, "fails once"));
        final Map<String, IndexMutation> documents = new LinkedHashMap<>();
        documents.put("doc", changes);
        documents.put("failing", failing);
        final Map<String, Map<String, IndexMutation>> mutations = new HashMap<>();
        mutations.put(store, documents);
        final BaseTransactionConfigurable providerTx = index.beginTransaction(
            StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        FailingOnceAnalyzer.FAIL.set(true);
        try {
            assertThrows(TemporaryBackendException.class, () -> index.mutate(mutations, retriever, providerTx));
        } finally {
            FailingOnceAnalyzer.FAIL.set(false);
        }
        index.mutate(mutations, retriever, providerTx);
        providerTx.commit();

        IndexTransaction query = openTx(retriever);
        assertEquals(ImmutableSet.of("doc"), query(query, store, ALIASES, Text.CONTAINS, "zed"));
        assertEquals(ImmutableSet.of("doc"), query(query, store, NAMES, Text.CONTAINS, "zed"));
        assertEquals(ImmutableSet.of("doc"), query(query, store, NAMES, Cmp.EQUAL, "Zed"));
        assertEquals(ImmutableSet.of(), query(query, store, NAMES, Cmp.EQUAL, "zed"));
        assertEquals(ImmutableSet.of("doc"), query(query, store, TAGS, Text.CONTAINS, "tag"));
        assertEquals(ImmutableSet.of("failing"), query(query, store, FAILING, Text.CONTAINS, "fails"));
        query.commit();
        final IndexTransaction removal = openTx(retriever);
        removal.delete(store, "doc", ALIASES, "new", false);
        removal.delete(store, "doc", NAMES, "New", false);
        removal.delete(store, "doc", TAGS, "new", false);
        removal.commit();
        query = openTx(retriever);
        for (final String key : new String[]{ALIASES, NAMES, TAGS}) {
            assertEquals(ImmutableSet.of(), query(query, store, key, Text.CONTAINS, "new"), key);
        }
        query.commit();
    }

    // A store which fails before it commits, in a mutation or in a restore, leaves none of its changes in its writer,
    // where the next commit of the store, as of another transaction, would make them last
    @Test
    public void failedStoreLeavesNoChangeBehind() throws Exception {
        final String store = "vertex";
        final KeyInformation.IndexRetriever retriever = getIndexRetriever(reattemptKeys());
        final IndexMutation written = new IndexMutation(retriever.get(store), true, false);
        written.addition(new IndexEntry(TAGS, "mutated"));
        final IndexMutation failing = new IndexMutation(retriever.get(store), true, false);
        failing.addition(new IndexEntry(FAILING, "fails once"));
        final Map<String, IndexMutation> documents = new LinkedHashMap<>();
        documents.put("mutated", written);
        documents.put("failing", failing);
        final Map<String, Map<String, IndexMutation>> mutations = new HashMap<>();
        mutations.put(store, documents);
        final Map<String, List<IndexEntry>> restored = new LinkedHashMap<>();
        restored.put("restored", Collections.singletonList(new IndexEntry(TAGS, "restored")));
        restored.put("failing", Collections.singletonList(new IndexEntry(FAILING, "fails once")));
        final BaseTransactionConfigurable providerTx = index.beginTransaction(
            StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        try {
            FailingOnceAnalyzer.FAIL.set(true);
            assertThrows(TemporaryBackendException.class, () -> index.mutate(mutations, retriever, providerTx));
            addAndCommit(retriever, store, "afterMutation");
            FailingOnceAnalyzer.FAIL.set(true);
            assertThrows(TemporaryBackendException.class, () -> index.restore(Collections.singletonMap(store, restored),
                retriever, providerTx));
            addAndCommit(retriever, store, "afterRestore");
        } finally {
            FailingOnceAnalyzer.FAIL.set(false);
        }
        providerTx.rollback();

        final IndexTransaction query = openTx(retriever);
        assertEquals(ImmutableSet.of(), query(query, store, TAGS, Text.CONTAINS, "mutated"));
        assertEquals(ImmutableSet.of(), query(query, store, TAGS, Text.CONTAINS, "restored"));
        assertEquals(ImmutableSet.of("afterMutation", "afterRestore"),
            query(query, store, TAGS, Text.CONTAINS, "other"));
        query.commit();
    }

    // A writer which a tragic event closed, as a background merge failing on a full disk, is replaced by the next write
    // of its store rather than failing it. Closing the index closes its writers in the same way and keeps them
    @Test
    public void writeReplacesAClosedWriter() throws Exception {
        final String store = "vertex";
        final KeyInformation.IndexRetriever retriever = getIndexRetriever(reattemptKeys());
        addAndCommit(retriever, store, "before");
        index.close();
        addAndCommit(retriever, store, "after");
        final IndexTransaction query = openTx(retriever);
        assertEquals(ImmutableSet.of("before", "after"), query(query, store, TAGS, Text.CONTAINS, "other"));
        query.commit();
    }

    //Another transaction's change of the store, whose commit commits what the store's writer holds
    private void addAndCommit(KeyInformation.IndexRetriever retriever, String store, String document)
            throws BackendException {
        final IndexTransaction other = openTx(retriever);
        other.add(store, document, TAGS, "other", true);
        other.commit();
    }

    //A LIST and a SET key of words, a SET key of words and whole strings, and a key whose analysis fails once when
    //FailingOnceAnalyzer is armed, as an I/O failure of the index would
    private static Map<String, KeyInformation> reattemptKeys() {
        final Map<String, KeyInformation> keys = new HashMap<>();
        keys.put(TAGS, of(String.class, Cardinality.LIST, Mapping.TEXT.asParameter()));
        keys.put(ALIASES, of(String.class, Cardinality.SET, Mapping.TEXT.asParameter()));
        keys.put(NAMES, of(String.class, Cardinality.SET, Mapping.TEXTSTRING.asParameter()));
        keys.put(FAILING, of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter(),
            new Parameter<>(ParameterType.TEXT_ANALYZER.getName(), FailingOnceAnalyzer.class.getName())));
        return keys;
    }

    private IndexTransaction openTx(KeyInformation.IndexRetriever retriever) throws BackendException {
        return new IndexTransaction(index, retriever, StandardBaseTransactionConfig.of(TimestampProviders.MILLI),
            Duration.ofMillis(2000L));
    }

    private static Set<String> query(IndexTransaction tx, String store, String key, JanusGraphPredicate predicate,
                                     Object value) throws BackendException {
        return tx.queryStream(new IndexQuery(store, PredicateCondition.of(key, predicate, value)))
            .collect(Collectors.toSet());
    }

    // An analyzer which fails the first analysis after it is armed, as an I/O failure of the index would
    public static final class FailingOnceAnalyzer extends Analyzer {
        private static final AtomicBoolean FAIL = new AtomicBoolean();

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer source = new WhitespaceTokenizer();
            return new TokenStreamComponents(source, new FailingFilter(source));
        }

        private static final class FailingFilter extends TokenFilter {
            private FailingFilter(TokenStream input) {
                super(input);
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (FAIL.getAndSet(false)) {
                    throw new IOException("An analysis which fails once");
                }
                return input.incrementToken();
            }
        }
    }

    private Set<String> rawQuery(String store, String query) throws BackendException {
        return tx.queryStream(new RawQuery(store, query, new Parameter[0])).map(RawQuery.Result::getResult)
            .collect(Collectors.toSet());
    }
}
