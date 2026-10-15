// Copyright 2026 JanusGraph Authors
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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A value removed from a key of several values leaves its Lucene document, as does a value of a SET key which an
 * addition of it replaces, also where the document stores the value in another form than the value's own string. A
 * float is stored as the double which it widens to, whose decimal string differs from the float's for most floats, and
 * a string in UTF-8, with U+FFFD in place of each unpaired surrogate.
 */
public class LuceneIndexValueRemovalTest {

    private static final String STORE = "vertex";
    private static final String KEY = "weights";

    //Each of them but 1.5f, the zeros, NaN and the infinities has a decimal string other than the double's which it
    //widens to, such as 0.10000000149011612 for 0.1f
    private static final float[] FLOATS = {0.1f, -0.3f, 3.14f, Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE,
        1.5f, 0.0f, -0.0f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};

    //The name, data type and mapping of a key of each data type, the value which its document keeps, the one which is
    //removed, and the kept value as Lucene reads it back
    private static final Object[][] KEYS = {
        {"byte", Byte.class, Mapping.DEFAULT, (byte) 7, Byte.MIN_VALUE, "7"},
        {"short", Short.class, Mapping.DEFAULT, (short) 7, Short.MIN_VALUE, "7"},
        {"int", Integer.class, Mapping.DEFAULT, 7, Integer.MIN_VALUE, "7"},
        {"long", Long.class, Mapping.DEFAULT, Long.MAX_VALUE, Long.MIN_VALUE, "9223372036854775807"},
        {"float", Float.class, Mapping.DEFAULT, -0.0f, 0.1f, "-0.0"},
        {"double", Double.class, Mapping.DEFAULT, -0.0, 0.0, "-0.0"},
        {"date", Date.class, Mapping.DEFAULT, new Date(7), new Date(-1), "7"},
        {"instant", Instant.class, Mapping.DEFAULT, Instant.ofEpochSecond(0, 7_000_001), Instant.ofEpochMilli(-1), "7"},
        {"boolean", Boolean.class, Mapping.DEFAULT, false, true, "0"},
        {"uuid", UUID.class, Mapping.DEFAULT, new UUID(7, 7), new UUID(1, 1), new UUID(7, 7).toString()},
        {"defaultString", String.class, Mapping.DEFAULT, "Kept Words", "Removed Words", "kept words"},
        {"text", String.class, Mapping.TEXT, "Kept Words", "Removed Words", "kept words"},
        {"string", String.class, Mapping.STRING, "Kept Words", "Removed Words", "Kept Words"},
        {"textString", String.class, Mapping.TEXTSTRING, "Kept Words", "Removed Words", "kept words"}};

    private LuceneIndex index;

    @BeforeEach
    public void setUp() throws BackendException {
        index = new LuceneIndex(LuceneIndexTest.getLocalLuceneTestConfig());
        index.clearStorage();
        index.close();
        index = new LuceneIndex(LuceneIndexTest.getLocalLuceneTestConfig());
    }

    @AfterEach
    public void tearDown() throws BackendException {
        index.close();
    }

    private IndexTransaction openTx(Map<String, KeyInformation> keys) throws BackendException {
        return new IndexTransaction(index, IndexProviderTest.getIndexRetriever(keys),
            StandardBaseTransactionConfig.of(TimestampProviders.MILLI), Duration.ofMillis(2000L));
    }

    private IndexTransaction openTx(KeyInformation key) throws BackendException {
        return openTx(Collections.singletonMap(KEY, key));
    }

    //One document keeps 2.5f, and the other one none of its floats
    @ParameterizedTest
    @ValueSource(strings = {"LIST", "SET"})
    public void testRemovedFloatsLeaveTheDocument(Cardinality cardinality) throws Exception {
        final KeyInformation key = IndexProviderTest.of(Float.class, cardinality);
        IndexTransaction tx = openTx(key);
        for (final float value : FLOATS) {
            tx.add(STORE, "some", KEY, value, true);
            tx.add(STORE, "none", KEY, value, true);
        }
        tx.add(STORE, "some", KEY, 2.5f, true);
        tx.commit();
        tx = openTx(key);
        for (final float value : FLOATS) {
            tx.delete(STORE, "some", KEY, value, false);
            tx.delete(STORE, "none", KEY, value, false);
        }
        tx.commit();

        assertEquals(Collections.singletonList("2.5"), storedValues("some", KEY));
        assertEquals(Collections.emptyList(), storedValues("none", KEY));
        tx = openTx(key);
        for (final float value : FLOATS) {
            final PredicateCondition<String, JanusGraphElement> equal = PredicateCondition.of(KEY, Cmp.EQUAL, value);
            assertEquals(Collections.emptySet(), query(tx, equal), Float.toString(value));
            assertEquals(0L, count(tx, equal), Float.toString(value));
        }
        assertEquals(Collections.singleton("some"), query(tx, PredicateCondition.of(KEY, Cmp.EQUAL, 2.5f)));
        tx.rollback();
    }

    //A SET holds a value once, which an addition of it replaces, and a LIST as often as it was added, of which a
    //removal takes out one
    @ParameterizedTest
    @ValueSource(strings = {"LIST", "SET"})
    public void testAFloatAddedAgainIsHeldOnceByASetAndTwiceByAList(Cardinality cardinality) throws Exception {
        final KeyInformation key = IndexProviderTest.of(Float.class, cardinality);
        final String stored = String.valueOf((double) 0.1f);
        IndexTransaction tx = openTx(key);
        tx.add(STORE, "doc", KEY, 0.1f, true);
        tx.commit();
        tx = openTx(key);
        tx.add(STORE, "doc", KEY, 0.1f, false);
        tx.commit();
        final List<String> held = cardinality == Cardinality.SET ? Collections.singletonList(stored)
            : Arrays.asList(stored, stored);
        assertEquals(held, storedValues("doc", KEY));

        tx = openTx(key);
        tx.delete(STORE, "doc", KEY, 0.1f, false);
        tx.commit();
        assertEquals(held.subList(1, held.size()), storedValues("doc", KEY));
        tx = openTx(key);
        assertEquals(cardinality == Cardinality.SET ? Collections.emptySet() : Collections.singleton("doc"),
            query(tx, PredicateCondition.of(KEY, Cmp.EQUAL, 0.1f)));
        tx.rollback();
    }

    //Strings with an unpaired surrogate, and one with a pair, which UTF-8 holds as it is
    @ParameterizedTest
    @CsvSource({"LIST, DEFAULT", "LIST, TEXT", "LIST, STRING", "LIST, TEXTSTRING",
        "SET, DEFAULT", "SET, TEXT", "SET, STRING", "SET, TEXTSTRING"})
    public void testRemovedStringsWithUnpairedSurrogatesLeaveTheDocument(Cardinality cardinality, Mapping mapping)
            throws Exception {
        final KeyInformation key = IndexProviderTest.of(String.class, cardinality, mapping.asParameter());
        final String[] removed = {"Ab\uD800c", "\uDC00", "x\uD83D\uDE00y"};
        IndexTransaction tx = openTx(key);
        for (final String value : removed) {
            tx.add(STORE, "doc", KEY, value, true);
        }
        tx.add(STORE, "doc", KEY, "Kept", true);
        tx.commit();
        assertStringsFound(key, mapping, removed, Collections.singleton("doc"));
        tx = openTx(key);
        for (final String value : removed) {
            tx.delete(STORE, "doc", KEY, value, false);
        }
        tx.commit();

        assertEquals(Collections.singletonList(mapping == Mapping.STRING ? "Kept" : "kept"), storedValues("doc", KEY));
        final Optional<String> wholeStrings = LuceneIndex.getDualFieldName(KEY, key);
        if (wholeStrings.isPresent()) {
            assertEquals(Collections.singletonList("Kept"), storedValues("doc", wholeStrings.get()));
        }
        assertStringsFound(key, mapping, removed, Collections.emptySet());
    }

    //The documents found by eq of each of the strings, on a key of whole strings, and by a word of the first one, on a
    //key of words
    private void assertStringsFound(KeyInformation key, Mapping mapping, String[] values, Set<String> expected)
            throws BackendException {
        final IndexTransaction tx = openTx(key);
        if (mapping == Mapping.STRING || mapping == Mapping.TEXTSTRING) {
            for (final String value : values) {
                assertEquals(expected, query(tx, PredicateCondition.of(KEY, Cmp.EQUAL, value)), value);
            }
        }
        if (mapping != Mapping.STRING) {
            assertEquals(expected, query(tx, PredicateCondition.of(KEY, Text.CONTAINS, "ab")));
        }
        tx.rollback();
    }

    //Each key keeps the value which wasn't removed, which it holds before the removed one, so that a comparison which
    //took the two for one value, as one of -0.0 and 0.0 by their numbers would, removes the kept value instead
    @ParameterizedTest
    @ValueSource(strings = {"LIST", "SET"})
    public void testRemovedValuesOfEachDataTypeLeaveTheDocument(Cardinality cardinality) throws Exception {
        final Map<String, KeyInformation> keys = new HashMap<>();
        for (final Object[] key : KEYS) {
            keys.put((String) key[0],
                IndexProviderTest.of((Class<?>) key[1], cardinality, ((Mapping) key[2]).asParameter()));
        }
        IndexTransaction tx = openTx(keys);
        for (final Object[] key : KEYS) {
            tx.add(STORE, "doc", (String) key[0], key[3], true);
            tx.add(STORE, "doc", (String) key[0], key[4], true);
        }
        tx.commit();
        tx = openTx(keys);
        for (final Object[] key : KEYS) {
            tx.delete(STORE, "doc", (String) key[0], key[4], false);
        }
        tx.commit();

        for (final Object[] key : KEYS) {
            assertEquals(Collections.singletonList(key[5]), storedValues("doc", (String) key[0]), (String) key[0]);
        }
        assertEquals(Collections.singletonList("Kept Words"),
            storedValues("doc", LuceneIndex.getDualFieldName("textString", keys.get("textString")).get()));
    }

    //The values of the field which the store's document holds, as Lucene reads them back
    private static List<String> storedValues(String document, String field) throws IOException {
        try (FSDirectory directory = FSDirectory.open(new File(StorageSetup.getHomeDir("lucene"), STORE).toPath());
             DirectoryReader reader = DirectoryReader.open(directory)) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            final TopDocs hits = searcher.search(new TermQuery(new Term(LuceneIndex.DOCID, document)), 2);
            assertEquals(1, hits.scoreDocs.length, document);
            return Arrays.asList(searcher.storedFields().document(hits.scoreDocs[0].doc).getValues(field));
        }
    }

    private static Set<String> query(IndexTransaction tx, PredicateCondition<String, JanusGraphElement> condition)
            throws BackendException {
        return tx.queryStream(new IndexQuery(STORE, condition)).collect(Collectors.toSet());
    }

    private static long count(IndexTransaction tx, PredicateCondition<String, JanusGraphElement> condition)
            throws BackendException {
        return tx.queryAggregation(new IndexQuery(STORE, condition), Aggregation.COUNT).longValue();
    }
}
