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
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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

    private Set<String> rawQuery(String store, String query) throws BackendException {
        return tx.queryStream(new RawQuery(store, query, new Parameter[0])).map(RawQuery.Result::getResult)
            .collect(Collectors.toSet());
    }
}
