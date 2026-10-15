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

import com.google.common.collect.Sets;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * has(key) reaches Lucene as an exists query. Lucene indexes a number, a date, an instant or a boolean as a point,
 * along with a doc value when its key holds a single value, and a string or a UUID as text with norms. The exists
 * query finds the documents which hold a value of a key of each data type and cardinality, also those which hold only
 * the least or only the greatest value of its type, and its negation finds the others. They do so in an index which
 * Lucene 9 created and in one which tells Lucene 8 as its creator, as an index of JanusGraph 0.6.0 to 1.1.x does, whose
 * fields Lucene 9 doesn't hold to one set of data structures.
 */
public class LuceneIndexExistsQueryTest {

    private static final String STORE = "vertex";

    //The name, data type and mapping of each key, and the least and the greatest of its values; Lucene orders NaN above
    //every other decimal
    private static final Object[][] KEYS = {
        {"byte", Byte.class, Mapping.DEFAULT, Byte.MIN_VALUE, Byte.MAX_VALUE},
        {"short", Short.class, Mapping.DEFAULT, Short.MIN_VALUE, Short.MAX_VALUE},
        {"int", Integer.class, Mapping.DEFAULT, Integer.MIN_VALUE, Integer.MAX_VALUE},
        {"long", Long.class, Mapping.DEFAULT, Long.MIN_VALUE, Long.MAX_VALUE},
        {"float", Float.class, Mapping.DEFAULT, Float.NEGATIVE_INFINITY, Float.NaN},
        {"double", Double.class, Mapping.DEFAULT, Double.NEGATIVE_INFINITY, Double.NaN},
        {"date", Date.class, Mapping.DEFAULT, new Date(Long.MIN_VALUE), new Date(Long.MAX_VALUE)},
        {"instant", Instant.class, Mapping.DEFAULT, Instant.ofEpochMilli(Long.MIN_VALUE),
            Instant.ofEpochMilli(Long.MAX_VALUE)},
        {"boolean", Boolean.class, Mapping.DEFAULT, false, true},
        {"uuid", UUID.class, Mapping.DEFAULT, new UUID(0, 0), new UUID(-1, -1)},
        {"defaultString", String.class, Mapping.DEFAULT, "", "Some Words"},
        {"text", String.class, Mapping.TEXT, "", "Some Words"},
        {"string", String.class, Mapping.STRING, "", "Some Words"},
        {"textString", String.class, Mapping.TEXTSTRING, "", "Some Words"}};

    private LuceneIndex index;
    private KeyInformation.IndexRetriever keys;

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

    private IndexTransaction openTx() throws BackendException {
        return new IndexTransaction(index, keys, StandardBaseTransactionConfig.of(TimestampProviders.MILLI),
            Duration.ofMillis(2000L));
    }

    //Each key has a document with its least value, one with its greatest, one with both when it holds several values,
    //and one whose values were removed; the negation finds the documents of the other keys and the one whose values
    //were removed
    @ParameterizedTest
    @CsvSource({"SINGLE, 9", "LIST, 9", "SET, 9", "SINGLE, 8", "LIST, 8", "SET, 8"})
    public void testExistsQueryFindsTheDocumentsWithAValueOfTheKey(Cardinality cardinality, int createdVersionMajor)
            throws Exception {
        final File store = new File(StorageSetup.getHomeDir("lucene"), STORE);
        assertTrue(store.mkdirs() || store.isDirectory());
        try (FSDirectory directory = FSDirectory.open(store.toPath());
             IndexWriter writer = new IndexWriter(directory,
                 new IndexWriterConfig().setIndexCreatedVersionMajor(createdVersionMajor))) {
            writer.commit();
        }
        final Map<String, KeyInformation> keyInformation = new HashMap<>();
        for (final Object[] key : KEYS) {
            keyInformation.put((String) key[0],
                IndexProviderTest.of((Class<?>) key[1], cardinality, ((Mapping) key[2]).asParameter()));
        }
        keys = IndexProviderTest.getIndexRetriever(keyInformation);
        final boolean severalValues = cardinality != Cardinality.SINGLE;

        final Set<String> documents = new HashSet<>();
        IndexTransaction tx = openTx();
        for (final Object[] key : KEYS) {
            final String name = (String) key[0];
            tx.add(STORE, name + "Least", name, key[3], true);
            tx.add(STORE, name + "Greatest", name, key[4], true);
            tx.add(STORE, name + "Removed", name, key[4], true);
            documents.addAll(Arrays.asList(name + "Least", name + "Greatest", name + "Removed"));
            if (severalValues) {
                tx.add(STORE, name + "Both", name, key[3], true);
                tx.add(STORE, name + "Both", name, key[4], true);
                tx.add(STORE, name + "Removed", name, key[3], true);
                documents.add(name + "Both");
            }
        }
        tx.commit();
        tx = openTx();
        for (final Object[] key : KEYS) {
            final String name = (String) key[0];
            tx.delete(STORE, name + "Removed", name, key[4], false);
            if (severalValues) {
                tx.delete(STORE, name + "Removed", name, key[3], false);
            }
        }
        tx.commit();

        tx = openTx();
        for (final Object[] key : KEYS) {
            final String name = (String) key[0];
            final Set<String> withValue = new HashSet<>(Arrays.asList(name + "Least", name + "Greatest"));
            if (severalValues) {
                withValue.add(name + "Both");
            }
            final PredicateCondition<String, JanusGraphElement> exists = PredicateCondition.of(name, Cmp.NOT_EQUAL,
                null);
            assertEquals(withValue, query(tx, exists), name);
            assertEquals((long) withValue.size(),
                tx.queryAggregation(new IndexQuery(STORE, exists), Aggregation.COUNT).longValue(), name);
            assertEquals(Sets.difference(documents, withValue), query(tx, Not.of(exists)), name);
        }
        tx.rollback();
    }

    private static Set<String> query(IndexTransaction tx, Condition<JanusGraphElement> condition)
            throws BackendException {
        return tx.queryStream(new IndexQuery(STORE, condition)).collect(Collectors.toSet());
    }
}
