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

package org.janusgraph.diskstorage.solr;

import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.params.CommonParams;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public abstract class SolrIndexTest extends IndexProviderTest {

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new SolrIndex(getSolrTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public String getEnglishAnalyzerName() {
        return EnglishAnalyzer.class.getName();
    }

    @Override
    public String getKeywordAnalyzerName() {
        return KeywordAnalyzer.class.getName();
    }

    protected abstract Configuration getSolrTestConfig();

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING))));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXTSTRING))));

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT))));

        assertTrue(index.supports(of(Long.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Long.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.DEFAULT))));
        assertTrue(index.supports(of(Integer.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Short.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Byte.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Float.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Object.class, Cardinality.SINGLE)));
        assertFalse(index.supports(of(Exception.class, Cardinality.SINGLE)));

        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.DEFAULT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXTSTRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT)), Text.CONTAINS_PHRASE));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.DEFAULT)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Cmp.NOT_EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXTSTRING)), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.DEFAULT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT)), Cmp.LESS_THAN));

        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.INTERSECT));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.CONTAINS));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.DISJOINT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.INTERSECT));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.DISJOINT));

        assertFalse(index.supports(of(Double.class, Cardinality.SINGLE), Geo.INTERSECT));
        assertFalse(index.supports(of(Long.class, Cardinality.SINGLE), Text.CONTAINS));

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
    }

    //Solr sorts by the string type of a STRING key and of the dual field of a TEXTSTRING key, whatever the analyzer,
    //and by numbers, dates, booleans and UUIDs, and not by the whole strings of a TEXT key, of a text type
    @Test
    public void testOrderingSupport() {
        assertTrue(index.supportsOrdering(allKeys.get(NAME)));
        assertTrue(index.supportsOrdering(allKeys.get(STRING)));
        assertTrue(index.supportsOrdering(allKeys.get(TEXT_STRING)));
        assertTrue(index.supportsOrdering(allKeys.get(FULL_TEXT)));
        assertFalse(index.supportsOrdering(allKeys.get(TEXT)));
        assertFalse(index.supportsOrdering(allKeys.get(ANALYZED)));
        assertFalse(index.supportsOrdering(of(String.class, Cardinality.SINGLE)));
        for (final Class<?> dataType : ImmutableList.of(Byte.class, Short.class, Integer.class, Long.class, Float.class,
                Double.class, Date.class, Boolean.class, UUID.class)) {
            assertTrue(index.supportsOrdering(of(dataType, Cardinality.SINGLE)), dataType.toString());
        }
        assertTrue(index.supportsOrdering(allKeys.get(DATE)), "an Instant");
        assertFalse(index.supportsOrdering(of(Geoshape.class, Cardinality.SINGLE)));
    }

    /*
     * Dropping collection is not implemented with Solr Cloud to accommodate use case where collection is created
     * outside of JanusGraph and associated with a config set with a different name.
     */
    @Override @Test @Disabled
    public void clearStorageTest() throws Exception {
        super.clearStorageTest();
    }

    @Test
    public void testMapKey2Field_IllegalCharacter() {
        assertThrows(IllegalArgumentException.class, () -> {

            KeyInformation keyInfo = new StandardKeyInformation(Boolean.class, Cardinality.SINGLE);
            index.mapKey2Field("here is an illegal character: •", keyInfo);
        });
    }

    @Test
    public void testMapKey2Field_MappingSpaces() {
        KeyInformation keyInfo = new StandardKeyInformation(Boolean.class, Cardinality.SINGLE);
        assertEquals("field•name•with•spaces_b", index.mapKey2Field("field name with spaces", keyInfo));
    }

    @Test
    public void testTextStringMapping() throws Exception {
        initialize("vertex");

        Multimap<String, Object> firstDoc = HashMultimap.create();
        firstDoc.put(TEXT_STRING, "John Doe");

        Multimap<String, Object> secondDoc = HashMultimap.create();
        secondDoc.put(TEXT_STRING, "John");

        add("vertex", "test1", firstDoc, true);
        add("vertex", "test2", secondDoc, true);

        clopen();

        assertEquals(1, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT_STRING, Cmp.EQUAL, "John"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT_STRING, Cmp.EQUAL, "John Doe"))).count());
        assertEquals(2, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT_STRING, Text.CONTAINS, "John"))).count());
    }

    @Test
    public void testTextStringSort() throws Exception {
        initialize("vertex");

        Multimap<String, Object> firstDoc = HashMultimap.create();
        firstDoc.put(TEXT_STRING, "John Doe");

        Multimap<String, Object> secondDoc = HashMultimap.create();
        secondDoc.put(TEXT_STRING, "Jane Doe");

        add("vertex", "test1", firstDoc, true);
        add("vertex", "test2", secondDoc, true);

        clopen();

        Object[] result = tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT_STRING, Text.CONTAINS, "Doe"),
            ImmutableList.of(new IndexQuery.OrderEntry(TEXT_STRING, Order.ASC, String.class)))).toArray();
        assertEquals("test2", result[0]);
        assertEquals("test1", result[1]);

        result = tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT_STRING, Text.CONTAINS, "Doe"),
            ImmutableList.of(new IndexQuery.OrderEntry(TEXT_STRING, Order.DESC, String.class)))).toArray();
        assertEquals("test1", result[0]);
        assertEquals("test2", result[1]);
    }

    // A value of a LIST key which a transaction removes and adds back, as a property replaced by one of the same value,
    // stays held once. The mutation's consolidation drops the removal, so the document still holds the value; added
    // with Solr's atomic "add", it would be held twice, and a later removal, which takes out one occurrence, would
    // leave it behind
    @Test
    public void listValueRemovedAndAddedBackIsHeldOnce() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> doc = HashMultimap.create();
        doc.put(PHONE_LIST, "1");
        doc.put(PHONE_LIST, "2");
        add(store, "doc", doc, true);
        clopen();
        tx.delete(store, "doc", PHONE_LIST, "1", false);
        tx.add(store, "doc", PHONE_LIST, "1", false);
        clopen();
        tx.delete(store, "doc", PHONE_LIST, "1", false);
        clopen();

        assertEquals(0, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "1")))
            .count());
        assertEquals(1, tx.queryStream(new IndexQuery(store, PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "2")))
            .count());
    }

    // A string-analyzer or text-analyzer parameter may name a tokenizer, as the documentation of the field mappings
    // has it: the tokenizer alone splits the value of a condition, as before JanusGraph took analyzers, since when the
    // cast to an analyzer failed. The fields text and xtext of the test configset are text_general, which splits these
    // values as the tokenizers do and puts them in lower case
    @Test
    public void analyzerParameterMayNameATokenizer() throws Exception {
        final String store = "vertex";
        initialize(store);
        final String words = "xtext";
        final Map<String, KeyInformation> keys = new HashMap<>();
        keys.put(TEXT, new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter(),
            new Parameter<>(ParameterType.TEXT_ANALYZER.getName(), WhitespaceTokenizer.class.getName())));
        keys.put(words, new StandardKeyInformation(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter(),
            new Parameter<>(ParameterType.STRING_ANALYZER.getName(), StandardTokenizer.class.getName())));
        final IndexTransaction addition = openTx(getIndexRetriever(keys));
        for (final Map.Entry<String, KeyInformation> key : keys.entrySet()) {
            addition.register(store, key.getKey(), key.getValue());
        }
        for (final String[] doc : new String[][]{{"tomAndJerry", "Tom and Jerry"}, {"spike", "Spike"}}) {
            addition.add(store, doc[0], TEXT, doc[1], true);
            addition.add(store, doc[0], words, doc[1], true);
        }
        addition.commit();

        final IndexTransaction query = openTx(getIndexRetriever(keys));
        assertEquals(ImmutableSet.of("tomAndJerry"), find(query, store, TEXT, Text.CONTAINS, "Jerry Tom"));
        assertEquals(ImmutableSet.of("tomAndJerry"), find(query, store, TEXT, Text.CONTAINS_PHRASE, "Tom and"));
        assertEquals(ImmutableSet.of(), find(query, store, TEXT, Text.CONTAINS_PHRASE, "Jerry Tom"));
        assertEquals(ImmutableSet.of("tomAndJerry"), find(query, store, words, Cmp.EQUAL, "Jerry Tom"));
        assertEquals(ImmutableSet.of("spike"), find(query, store, words, Cmp.NOT_EQUAL, "Jerry Tom"));
        assertEquals(ImmutableSet.of(), find(query, store, words, Cmp.EQUAL, "!!!"));
        query.commit();
    }

    // A result larger than a page (max-result-set-size, 3 here) is read at offsets in pages which double from the first
    // one up to the largest page (max-page-size, 12 here), but never past the limit, nor past the documents which the
    // first page found: 3, 6, 12, 12 and 7 documents. A page is the last one where Solr found no more documents than
    // those up to its end, so a result which a page holds is one request. The requests are otherwise those of a single
    // page, with the query's sort alone, if any. A direct index query is read in the same pages from its offset, or
    // from the start which a parameter sets, also with other parameters of its own; one whose parameters set its rows
    // is read in pages of that size, also below max-result-set-size or beyond the limit, and its later pages ask for no
    // more than the limit leaves. Its totals take one request, and count the documents from the same start as its read
    @Test
    public void resultsLargerThanAPageAreReadInGrowingPages() throws Exception {
        final String store = "vertex";
        initialize(store);
        for (int i = 0; i < 40; i++) {
            final Multimap<String, Object> doc = HashMultimap.create();
            doc.put(TIME, (long) i);
            add(store, "d" + i, doc, true);
        }
        clopen();
        final ImmutableList<IndexQuery.OrderEntry> timeAsc =
            ImmutableList.of(new IndexQuery.OrderEntry(TIME, Order.ASC, Long.class));
        final PredicateCondition<String, JanusGraphElement> all =
            PredicateCondition.of(TIME, Cmp.GREATER_THAN_EQUAL, 0L);
        final RawQuery rawQuery = new RawQuery(store, "time:[0 TO *]", new Parameter[0]);

        try (Searches searches = new Searches()) {
            assertEquals(documents(0, 40),
                tx.queryStream(new IndexQuery(store, all, timeAsc)).collect(Collectors.toList()));
            assertPages(searches.next(), "time asc", 0, 3, 3, 6, 9, 12, 21, 12, 33, 7);
            assertEquals(documents(0, 20),
                tx.queryStream(new IndexQuery(store, all, timeAsc, 20)).collect(Collectors.toList()));
            assertPages(searches.next(), "time asc", 0, 3, 3, 6, 9, 11);
            assertEquals(ImmutableSet.copyOf(documents(0, 40)),
                tx.queryStream(new IndexQuery(store, all)).collect(Collectors.toSet()));
            assertPages(searches.next(), null, 0, 3, 3, 6, 9, 12, 21, 12, 33, 7);
            assertEquals(documents(0, 3),
                tx.queryStream(new IndexQuery(store, all, timeAsc, 3)).collect(Collectors.toList()));
            assertPages(searches.next(), "time asc", 0, 3);
            assertEquals(3,
                tx.queryStream(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.LESS_THAN, 3L))).count());
            assertPages(searches.next(), null, 0, 3);

            assertEquals(40, tx.queryStream(rawQuery).count());
            assertPages(searches.next(), null, 0, 3, 3, 6, 9, 12, 21, 12, 33, 7);
            assertEquals(38,
                tx.queryStream(new RawQuery(store, "time:[0 TO *]", new Parameter[0]).setOffset(2)).count());
            assertPages(searches.next(), null, 2, 3, 5, 6, 11, 12, 23, 12, 35, 5);
            assertEquals(10, tx.queryStream(new RawQuery(store, "time:[0 TO 9]",
                new Parameter[]{new Parameter<>("defType", "lucene")})).count());
            assertPages(searches.next(), null, 0, 3, 3, 6, 9, 1);
            assertEquals(5, tx.queryStream(new RawQuery(store, "time:[0 TO *]",
                new Parameter[]{new Parameter<>(CommonParams.ROWS, "2")}).setLimit(5)).count());
            assertPages(searches.next(), null, 0, 2, 2, 2, 4, 1);
            assertEquals(5, tx.queryStream(new RawQuery(store, "time:[0 TO *]",
                new Parameter[]{new Parameter<>(CommonParams.ROWS, "10")}).setLimit(5)).count());
            assertPages(searches.next(), null, 0, 10);
            final RawQuery fromStart = new RawQuery(store, "time:[0 TO *]",
                new Parameter[]{new Parameter<>(CommonParams.START, "24")});
            assertEquals(16, tx.queryStream(fromStart).count());
            assertPages(searches.next(), null, 24, 3, 27, 6, 33, 7);
            assertEquals(40L, tx.totals(rawQuery).longValue());
            assertPages(searches.next(), null, 0, 3);
            assertEquals(16L, tx.totals(fromStart).longValue());
            assertPages(searches.next(), null, 24, 3);
        }
    }

    private static List<String> documents(int from, int to) {
        return IntStream.range(from, to).mapToObj(i -> "d" + i).collect(Collectors.toList());
    }

    // The searches asked for these offsets and rows, in pairs, and sorted by the sort, or by none
    private static void assertPages(List<Map<String, String>> searches, String sort, int... startsAndRows) {
        assertEquals(startsAndRows.length / 2, searches.size(), searches.toString());
        for (int i = 0; i < searches.size(); i++) {
            final Map<String, String> search = searches.get(i);
            assertEquals(String.valueOf(startsAndRows[2 * i]), search.get(CommonParams.START), searches.toString());
            assertEquals(String.valueOf(startsAndRows[2 * i + 1]), search.get(CommonParams.ROWS),
                searches.toString());
            assertEquals(sort, search.get(CommonParams.SORT), searches.toString());
        }
    }

    // The parameters of the searches which this thread sends to Solr through the clients of SolrJ's HttpClientUtil
    private static final class Searches implements HttpRequestInterceptor, AutoCloseable {

        private final Thread thread = Thread.currentThread();
        private final List<Map<String, String>> searches = new ArrayList<>();

        private Searches() {
            HttpClientUtil.addRequestInterceptor(this);
        }

        @Override
        public void process(HttpRequest request, HttpContext context) {
            final URI uri = URI.create(request.getRequestLine().getUri());
            if (Thread.currentThread() == thread && uri.getPath().endsWith("/select")) {
                searches.add(URLEncodedUtils.parse(uri, StandardCharsets.UTF_8).stream()
                    .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue, (a, b) -> a + "," + b)));
            }
        }

        // The searches since the last call
        private List<Map<String, String>> next() {
            final List<Map<String, String>> next = new ArrayList<>(searches);
            searches.clear();
            return next;
        }

        @Override
        public void close() {
            HttpClientUtil.removeRequestInterceptor(this);
        }
    }

    private IndexTransaction openTx(KeyInformation.IndexRetriever retriever) throws BackendException {
        return new IndexTransaction(index, retriever, StandardBaseTransactionConfig.of(TimestampProviders.MILLI),
            Duration.ofMillis(2000L));
    }

    private static Set<String> find(IndexTransaction tx, String store, String key, JanusGraphPredicate predicate,
                                    String value) throws BackendException {
        return tx.queryStream(new IndexQuery(store, PredicateCondition.of(key, predicate, value)))
            .collect(Collectors.toSet());
    }

    @Override
    public Mapping preferredGeoShapeMapping() {
        return Mapping.PREFIX_TREE;
    }
}
