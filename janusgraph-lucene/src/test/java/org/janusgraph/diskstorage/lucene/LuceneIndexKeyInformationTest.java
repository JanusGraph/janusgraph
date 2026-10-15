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

import com.google.common.base.Preconditions;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.janusgraph.diskstorage.indexing.IndexProviderTest.KEYWORD;
import static org.janusgraph.diskstorage.indexing.IndexProviderTest.TEXT;
import static org.janusgraph.diskstorage.indexing.IndexProviderTest.getIndexRetriever;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The analyzer of a Lucene store is shared by every transaction, so it analyzes each field with the key information
 * of the transaction at hand, not of the one which first used the store. The index stays open between the
 * transactions of a test: reopening it would give the store a new analyzer, and hide a stale one.
 */
public class LuceneIndexKeyInformationTest {

    private LuceneIndex index;
    private Map<String, KeyInformation> allKeys;
    private IndexTransaction tx;

    @BeforeEach
    public void setUp() throws BackendException {
        index = new LuceneIndex(LuceneIndexTest.getLocalLuceneTestConfig());
        index.clearStorage();
        index.close();
        index = new LuceneIndex(LuceneIndexTest.getLocalLuceneTestConfig());
        allKeys = IndexProviderTest.getMapping(index.getFeatures(), EnglishAnalyzer.class.getName(),
            KeywordAnalyzer.class.getName(), Mapping.PREFIX_TREE);
        tx = openTx(getIndexRetriever(allKeys));
    }

    @AfterEach
    public void tearDown() throws BackendException {
        tx.rollback();
        index.close();
    }

    private IndexTransaction openTx(KeyInformation.IndexRetriever retriever) throws BackendException {
        return new IndexTransaction(index, retriever, StandardBaseTransactionConfig.of(TimestampProviders.MILLI),
            Duration.ofMillis(2000L));
    }

    private void newTx() throws BackendException {
        tx.commit();
        tx = openTx(getIndexRetriever(allKeys));
    }

    //A key added to the index after a transaction first used the store is unknown to that transaction, and the
    //analyzer configured for it has to come from the transaction which indexes it: the keyword analyzer here, which
    //keeps "hello world" one token, where the standard analyzer of an unknown key would split it
    @Test
    public void testAKeyAddedAfterTheFirstUseOfAStoreIsAnalyzedAsConfigured() throws Exception {
        final String store = "later";
        final Map<String, KeyInformation> keysBefore = new HashMap<>(allKeys);
        keysBefore.remove(KEYWORD);
        final IndexTransaction first = openTx(getIndexRetriever(keysBefore));
        first.add(store, "doc1", new IndexEntry(TEXT, "the first text"), true);
        first.commit();

        tx.add(store, "doc2", new IndexEntry(KEYWORD, "hello world"), true);
        newTx();

        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(KEYWORD, Text.CONTAINS, "hello world"))).count());
        assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(KEYWORD, Text.CONTAINS, "hello"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "first"))).count());
    }

    //The transaction which first used a store closes, and a closed transaction can't read the schema any more, so
    //its key information must not be consulted for the transactions after it
    @Test
    public void testTheKeyInformationOfAClosedTransactionIsLeftAlone() throws Exception {
        final String store = "closing";
        final AtomicBoolean closed = new AtomicBoolean();
        final KeyInformation.IndexRetriever untilClosed = new KeyInformation.IndexRetriever() {
            @Override
            public KeyInformation get(String s, String key) {
                return get(s).get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(String s) {
                Preconditions.checkState(!closed.get(), "The transaction has been closed");
                return getIndexRetriever(allKeys).get(s);
            }

            @Override
            public void invalidate(String s) {
            }
        };
        final IndexTransaction first = openTx(untilClosed);
        first.add(store, "doc1", new IndexEntry(TEXT, "the first text"), true);
        first.commit();
        closed.set(true);

        tx.add(store, "doc2", new IndexEntry(TEXT, "the second text"), true);
        newTx();

        assertEquals(2, tx.queryStream(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "text"))).count());
        assertEquals(1, tx.queryStream(new RawQuery(store, TEXT + ":second", new Parameter[0])).count());
    }
}
