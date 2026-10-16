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
import org.apache.lucene.util.BytesRef;
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
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.janusgraph.util.StringUtils.lowerCaseCodePoints;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Lucene's ByteBuffersDataOutput.writeLongString writes a stored string longer than 1,024 UTF-16 units in windows of
 * 1,023 units. A window which ends in a high surrogate takes one more unit, which it expects to be the second half of
 * the pair. When the high surrogate is unpaired and a surrogate pair follows it, that unit is the first half of the
 * pair instead. Lucene then writes U+FFFD for each half of the pair, 6 bytes where it counted 4.
 * <p>
 * Such a document can't be read back when another stored field follows the string. Otherwise it reads back with the
 * string cut short, and the next update of the document indexes the shortened string.
 * <p>
 * So LuceneIndex hands Lucene the string as Lucene stores it anyway, with U+FFFD in place of each unpaired surrogate.
 * It analyzes that string and converts query values the same way, since the standard tokenizer drops the last words
 * of a text which ends in an unpaired high surrogate.
 */
public class LuceneIndexUnpairedSurrogateTest {

    private static final String STORE = "vertex";
    private static final String KEY = "text";
    private static final String OTHER = "name";

    //An unpaired high surrogate as the 1,023rd unit, the last of Lucene's first window, and a surrogate pair after it
    private static final String VALUE = "x".repeat(1022) + "\uD800\uD83D\uDE00 Tail Words";

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

    //The string is written before or after the value of another key: in the first case Lucene failed to read the
    //document, as for a query of the other key, and in the second it read back the string without its last two bytes
    @ParameterizedTest
    @CsvSource({"DEFAULT, true", "DEFAULT, false", "TEXT, true", "TEXT, false", "STRING, true", "STRING, false",
        "TEXTSTRING, true", "TEXTSTRING, false"})
    public void testADocumentWithAStringOfAnUnpairedSurrogateBeforeAPairReadsBack(Mapping mapping, boolean first)
            throws Exception {
        final Map<String, KeyInformation> keys = new HashMap<>();
        keys.put(KEY, IndexProviderTest.of(String.class, Cardinality.SINGLE, mapping.asParameter()));
        keys.put(OTHER, IndexProviderTest.of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()));
        IndexTransaction tx = openTx(keys);
        if (first) {
            tx.add(STORE, "doc", KEY, VALUE, true);
        }
        tx.add(STORE, "doc", OTHER, "first", true);
        if (!first) {
            tx.add(STORE, "doc", KEY, VALUE, true);
        }
        tx.commit();
        assertEquals(Collections.singleton("doc"), query(keys, PredicateCondition.of(OTHER, Cmp.EQUAL, "first")));

        //An update reads the document back and indexes the string again from what Lucene stored
        tx = openTx(keys);
        tx.add(STORE, "doc", OTHER, "second", false);
        tx.commit();
        assertEquals(Collections.singleton("doc"), query(keys, PredicateCondition.of(OTHER, Cmp.EQUAL, "second")));
        if (mapping != Mapping.STRING) {
            assertEquals(Collections.singleton("doc"), query(keys, PredicateCondition.of(KEY, Text.CONTAINS, "words")));
        }
        if (mapping == Mapping.STRING || mapping == Mapping.TEXTSTRING) {
            assertEquals(Collections.singleton("doc"), query(keys, PredicateCondition.of(KEY, Cmp.EQUAL, VALUE)));
        }
        final String stored = mapping == Mapping.STRING ? VALUE : lowerCaseCodePoints(VALUE);
        assertEquals(Collections.singletonList(new BytesRef(stored).utf8ToString()), storedValues(KEY));
        final Optional<String> wholeStrings = LuceneIndex.getDualFieldName(KEY, keys.get(KEY));
        if (wholeStrings.isPresent()) {
            assertEquals(Collections.singletonList(new BytesRef(VALUE).utf8ToString()),
                storedValues(wholeStrings.get()));
        }
    }

    //The standard tokenizer drops the words which it reads last with an unpaired high surrogate at the end of the
    //text, all of them in a short string, which a later write of the document then found in the stored string
    @ParameterizedTest
    @ValueSource(strings = {"DEFAULT", "TEXT", "TEXTSTRING"})
    public void testTheWordsOfAStringWhichEndsInAnUnpairedHighSurrogateAreFound(Mapping mapping) throws Exception {
        final Map<String, KeyInformation> keys = Collections.singletonMap(KEY,
            IndexProviderTest.of(String.class, Cardinality.SINGLE, mapping.asParameter()));
        final IndexTransaction tx = openTx(keys);
        tx.add(STORE, "doc", KEY, "Tail Words\uD800", true);
        tx.add(STORE, "other", KEY, "Other Text", true);
        tx.commit();
        assertEquals(Collections.singleton("doc"), query(keys, PredicateCondition.of(KEY, Text.CONTAINS, "words")));
    }

    //A query value is analyzed as the indexed strings are: one which ends in an unpaired high surrogate gave the
    //standard tokenizer no words, so that textContains found every document and textContainsPrefix none
    @ParameterizedTest
    @ValueSource(strings = {"DEFAULT", "TEXT", "TEXTSTRING"})
    public void testAQueryValueWhichEndsInAnUnpairedHighSurrogateFindsItsWords(Mapping mapping) throws Exception {
        final Map<String, KeyInformation> keys = Collections.singletonMap(KEY,
            IndexProviderTest.of(String.class, Cardinality.SINGLE, mapping.asParameter()));
        final IndexTransaction tx = openTx(keys);
        tx.add(STORE, "doc", KEY, "Tail Words", true);
        tx.add(STORE, "other", KEY, "Other Text", true);
        tx.commit();
        assertEquals(Collections.singleton("doc"),
            query(keys, PredicateCondition.of(KEY, Text.CONTAINS, "words\uD800")));
        assertEquals(Collections.singleton("doc"),
            query(keys, PredicateCondition.of(KEY, Text.CONTAINS_PREFIX, "word\uD800")));
    }

    private IndexTransaction openTx(Map<String, KeyInformation> keys) throws BackendException {
        return new IndexTransaction(index, IndexProviderTest.getIndexRetriever(keys),
            StandardBaseTransactionConfig.of(TimestampProviders.MILLI), Duration.ofMillis(2000L));
    }

    private Set<String> query(Map<String, KeyInformation> keys, PredicateCondition<String, JanusGraphElement> condition)
            throws BackendException {
        final IndexTransaction tx = openTx(keys);
        try {
            return tx.queryStream(new IndexQuery(STORE, condition)).collect(Collectors.toSet());
        } finally {
            tx.rollback();
        }
    }

    //The values of the field which the document holds, as Lucene reads them back from the directory of the store
    private static List<String> storedValues(String field) throws IOException {
        final String indexDirectory = LuceneIndexTest.getLocalLuceneTestConfig()
            .get(GraphDatabaseConfiguration.INDEX_DIRECTORY);
        try (FSDirectory directory = FSDirectory.open(new File(indexDirectory, STORE).toPath());
             DirectoryReader reader = DirectoryReader.open(directory)) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            final TopDocs hits = searcher.search(new TermQuery(new Term(LuceneIndex.DOCID, "doc")), 2);
            assertEquals(1, hits.scoreDocs.length);
            return Arrays.asList(searcher.storedFields().document(hits.scoreDocs[0].doc).getValues(field));
        }
    }
}
