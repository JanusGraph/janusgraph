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

package org.janusgraph.diskstorage.es;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.util.stats.MetricManager;
import org.janusgraph.util.system.ConfigurationUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Testcontainers
public class ElasticsearchIndexTest extends IndexProviderTest {

    @Container
    public static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();
    static HttpHost host;
    static CloseableHttpClient httpClient;
    static ObjectMapper objectMapper;

    private static char REPLACEMENT_CHAR = '\u2022';

    @BeforeAll
    public static void prepareElasticsearch() throws Exception {
        httpClient = HttpClients.createDefault();
        objectMapper = new ObjectMapper();
        host = new HttpHost(InetAddress.getByName(esr.getHostname()), esr.getPort());
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("_ingest/pipeline/pipeline_1")));
        final HttpPut newPipeline = new HttpPut("_ingest/pipeline/pipeline_1");
        newPipeline.setHeader("Content-Type", "application/json");
        newPipeline.setEntity(new StringEntity("{\"description\":\"Test pipeline\",\"processors\":[{\"set\":{\"field\":\"" +STRING+ "\",\"value\":\"hello\"}}]}", StandardCharsets.UTF_8));
        IOUtils.closeQuietly(httpClient.execute(host, newPipeline));
    }

    @AfterAll
    public static void cleanupElasticsearch() throws IOException {
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("janusgraph*")));
        IOUtils.closeQuietly(httpClient);
    }

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new ElasticSearchIndex(getESTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public String getEnglishAnalyzerName() {
        return "english";
    }
    
    @Override
    public String getKeywordAnalyzerName() {
        return "keyword";
    }

    @Override
    public Mapping preferredGeoShapeMapping() {
        if(JanusGraphElasticsearchContainer.getEsMajorVersion().value <= 6){
            return Mapping.PREFIX_TREE;
        }
        return Mapping.BKD;
    }

    public Configuration getESTestConfig() {
        final String index = "es";
        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index." + index + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        return makeESTestConfig(index, cc);
    }

    public Configuration makeESTestConfig(String index, CommonsConfiguration cc) {
        return esr.setConfiguration(new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,cc, BasicConfiguration.Restriction.NONE), index)
            .set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index)
            .restrictTo(index);
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.NOT_CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.NOT_CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_PHRASE));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.NOT_CONTAINS_PHRASE));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.NOT_CONTAINS_REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.NOT_CONTAINS_FUZZY));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.NOT_REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.CONTAINS));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.NOT_CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.NOT_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.NOT_FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.NOT_REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Cmp.NOT_EQUAL));

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
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.DISJOINT));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.PREFIX_TREE.asParameter()), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.PREFIX_TREE.asParameter()), Geo.INTERSECT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.PREFIX_TREE.asParameter()), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.PREFIX_TREE.asParameter()), Geo.DISJOINT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.BKD.asParameter()), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.BKD.asParameter()), Geo.INTERSECT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.BKD.asParameter()), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, Mapping.BKD.asParameter()), Geo.DISJOINT));
    }

    @Test
    public void testErrorInBatch() throws Exception {
        initialize("vertex");
        Multimap<String, Object> doc1 = HashMultimap.create();
        doc1.put(TIME, "not a time");

        add("vertex", "failing-doc", doc1, true);
        add("vertex", "non-failing-doc", getRandomDocument(), true);

        JanusGraphException janusGraphException = assertThrows(JanusGraphException.class, () -> {
            tx.commit();
        }, "Commit should not have succeeded.");

        String message = Throwables.getRootCause(janusGraphException).getMessage();

        switch (JanusGraphElasticsearchContainer.getEsMajorVersion().value){
            case 7:
            case 6:
                assertTrue(message.contains("mapper_parsing_exception"));
                break;
            case 5:
                assertTrue(message.contains("number_format_exception"));
                break;
            default:
                assertTrue(message.contains("document_parsing_exception"));
                break;
        }

        tx = null;
    }

    //The document is present in the graph but absent from the index, and the transaction takes content out of it and
    //puts other content in. The addition is sent as an update without an upsert, because the mutation has deletions,
    //so Elasticsearch answers it with a 404 document_missing_exception: a lost write. Every 404 used to be taken for a
    //success, which left the addition unindexed with nothing reported
    @Test
    public void testAdditionToAMissingDocumentIsReported() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", documentWith(TIME, 1L), true);
        clopen();

        //Take the document out of the index behind the back of the transaction which follows
        tx.delete("vertex", "diverged", TIME, 1L, true);
        newTx();

        //A deletion of one field and an addition of another both survive consolidation, so the upsert is withheld
        tx.delete("vertex", "diverged", TIME, 1L, false);
        tx.add("vertex", "diverged", WEIGHT, 2.5, false);
        final JanusGraphException e = assertThrows(JanusGraphException.class, tx::commit,
            "Commit should not have succeeded.");
        tx = null;

        final Throwable rootCause = Throwables.getRootCause(e);
        assertTrue(rootCause instanceof ElasticSearchBulkFailureException, rootCause.toString());
        final ElasticSearchBulkFailureException failure = (ElasticSearchBulkFailureException) rootCause;
        //Only the addition is reported. Elasticsearch answers the field deletion with the same 404, but that one
        //asked for nothing an absent document does not already satisfy
        assertEquals(1, failure.getFailedItems().size(), failure.getFailedItems().toString());
        assertEquals(Collections.singleton(HttpStatus.SC_NOT_FOUND), failure.getFailedItemStatusCodes());
        assertTrue(failure.getFailedItems().get(0).toString().contains("document_missing_exception"),
            failure.getFailedItems().toString());
    }

    //Both mutations which only take content out of the index. Elasticsearch answers a whole document deletion of an
    //absent document with a 404 carrying no error, and the script which deletes fields with a 404
    //document_missing_exception. Neither is a lost write, because an absent document is already the state each asked
    //for, so neither is reported
    @Test
    public void testRemovingContentFromAMissingDocumentIsNotReported() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", documentWith(TIME, 1L), true);
        clopen();

        tx.delete("vertex", "diverged", TIME, 1L, true);
        newTx();

        //The field deletion script against the absent document. A reported item would make this commit throw
        tx.delete("vertex", "diverged", TIME, 1L, false);
        newTx();
        //The whole document deletion of the absent document
        tx.delete("vertex", "diverged", TIME, 1L, true);
        newTx();
    }

    //A value change of a SINGLE cardinality key is not the reported shape, although issue #4926 names it as the one:
    //IndexTransaction consolidates the mutation before mutate() sees it, and the deletion of the old value is dropped
    //because the same field is added. The addition then carries an upsert, which recreates the absent document from
    //the changed field alone
    @Test
    public void testValueChangeOfASingleCardinalityKeyRecreatesAMissingDocument() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", documentWith(TIME, 1L), true);
        clopen();

        tx.delete("vertex", "diverged", TIME, 1L, true);
        newTx();

        tx.delete("vertex", "diverged", TIME, 1L, false);
        tx.add("vertex", "diverged", TIME, 2L, false);
        clopen();

        assertEquals(1, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TIME, Cmp.EQUAL, 2L))).count());
    }

    //A removal against an index which no longer exists is reported. Elasticsearch answers it with a 404 as well, but
    //one which says the whole index is missing, and that is not the state any mutation asked for
    @Test
    public void testRemovalAgainstAMissingIndexIsReported() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", documentWith(TIME, 1L), true);
        clopen();

        //Drop the whole index behind the back of the transaction which follows
        final String indexStoreName = INDEX_NAME.getDefaultValue() + "_vertex";
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete(indexStoreName)));
        assertFalse(indexExists(indexStoreName));

        tx.delete("vertex", "diverged", TIME, 1L, true);
        final JanusGraphException e = assertThrows(JanusGraphException.class, tx::commit,
            "Commit should not have succeeded.");
        tx = null;

        final Throwable rootCause = Throwables.getRootCause(e);
        assertTrue(rootCause instanceof ElasticSearchBulkFailureException, rootCause.toString());
        final ElasticSearchBulkFailureException failure = (ElasticSearchBulkFailureException) rootCause;
        assertEquals(Collections.singleton(HttpStatus.SC_NOT_FOUND), failure.getFailedItemStatusCodes());
        //The reason is the type Elasticsearch names in the item's error map
        final Object error = failure.getFailedItems().get(0);
        assertTrue(error instanceof Map, String.valueOf(error));
        assertEquals("index_not_found_exception", ((Map<?, ?>) error).get("type"), String.valueOf(error));
    }

    //A read failure whose status is listed in retry-error-codes is classified transient, the way a write failure is,
    //so that BackendOperation reattempts it within storage.read-time. Nothing makes a 400 or a 404 transient in
    //practice, but they are the statuses a live Elasticsearch can be made to answer a search with on demand - a
    //malformed query string, and an index which does not exist - and the classification only asks whether the status
    //is on the list. Between them the two shapes reach all four read paths
    @Test
    public void testReadFailureWithAStatusInRetryErrorCodesIsTransient() throws Exception {
        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index.es.elasticsearch.retry-error-codes", "400,404");
        //No client level reattempts, so that the classification is what the test waits for
        cc.set("index.es.elasticsearch.retry-limit", "0");
        final ElasticSearchIndex transientOnListedStatuses = new ElasticSearchIndex(makeESTestConfig("es", cc));
        try {
            final BaseTransaction readTx = transientOnListedStatuses.beginTransaction(
                StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
            try {
                transientOnListedStatuses.register("vertex", TIME, allKeys.get(TIME), readTx);
                //An unbalanced query string, which Elasticsearch rejects with a 400: the raw query and count paths
                final RawQuery malformed = new RawQuery("vertex", TIME + ":(", new Parameter[0]);
                assertThrows(TemporaryBackendException.class,
                    () -> transientOnListedStatuses.query(malformed, indexRetriever, readTx));
                assertThrows(TemporaryBackendException.class,
                    () -> transientOnListedStatuses.totals(malformed, indexRetriever, readTx));

                //A well formed query against a store whose index does not exist, which Elasticsearch answers with a
                //404: the index query and aggregation paths
                final IndexQuery againstAMissingIndex = new IndexQuery("nosuchstore",
                    PredicateCondition.of(TIME, Cmp.EQUAL, 1L));
                assertThrows(TemporaryBackendException.class,
                    () -> transientOnListedStatuses.query(againstAMissingIndex, indexRetriever, readTx));
                assertThrows(TemporaryBackendException.class, () -> transientOnListedStatuses.queryAggregation(
                    againstAMissingIndex, indexRetriever, readTx, Aggregation.MIN(TIME)));
            } finally {
                readTx.rollback();
            }
        } finally {
            transientOnListedStatuses.close();
        }
    }

    //A reattempt after a temporary failure resends only the documents which did not apply. The good document has a
    //LIST value appended by the addition script, the one write a resend does not make idempotent: if the reattempts
    //resent it, the document would hold the value once per attempt
    @Test
    public void testAReattemptResendsOnlyTheDocumentsWhichDidNotApply() throws Exception {
        initialize("vertex");
        add("vertex", "good", documentWith(PHONE_LIST, "1"), true);
        clopen();

        //A 400 is not transient anywhere in practice, but it is the one item failure a live Elasticsearch can be made
        //to answer on demand, with a value the mapping rejects. Listing it makes the bulk failure temporary, so that
        //BackendOperation reattempts the mutation until the write time is used up
        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index.es.elasticsearch.retry-error-codes", "400");
        cc.set("index.es.elasticsearch.retry-limit", "0");
        final ElasticSearchIndex transientOn400 = new ElasticSearchIndex(makeESTestConfig("es", cc));
        try {
            //Metered, so that the number of attempts can be asserted: a run which gave up after a single attempt,
            //because a bulk under refresh=wait_for takes about a second, would prove nothing about reattempts
            final String metricsGroup = "ElasticsearchIndexTest.reattempts." + UUID.randomUUID();
            final IndexTransaction reattempting = new IndexTransaction(
                new MetricInstrumentedIndexProvider(transientOn400, "es"), indexRetriever,
                new StandardBaseTransactionConfig.Builder().timestampProvider(TimestampProviders.MILLI)
                    .groupName(metricsGroup).build(), Duration.ofSeconds(5));
            try {
                reattempting.add("vertex", "good", PHONE_LIST, "2", false);
                reattempting.add("vertex", "bad", TIME, "not a time", true);

                //commit runs the mutation through BackendOperation.execute, which wraps what ends the wait
                final JanusGraphException e = assertThrows(JanusGraphException.class, reattempting::commit);
                //The wait ended with a temporary failure; that it was reattempted is what the metric below shows
                assertTrue(e.getCause() instanceof TemporaryBackendException, String.valueOf(e.getCause()));
                final long attempts = MetricManager.INSTANCE.getCounter(metricsGroup, "es",
                    MetricInstrumentedIndexProvider.M_MUTATE, MetricInstrumentedIndexProvider.M_CALLS).getCount();
                assertTrue(attempts >= 2, "the mutation was attempted " + attempts + " time(s)");
            } finally {
                //The commit did not complete, so the transaction still holds the mutation it could not apply
                reattempting.rollback();
            }
        } finally {
            transientOn400.close();
        }

        assertEquals(Arrays.asList("1", "2"), listValues("vertex", "good", PHONE_LIST));
    }

    private List<Object> listValues(String store, String documentId, String field) throws Exception {
        final HttpGet get = new HttpGet(documentPath(store, documentId));
        try (CloseableHttpResponse response = httpClient.execute(host, get)) {
            final String body = EntityUtils.toString(response.getEntity());
            //A missing document is a failure of the test, said as such rather than as a cast of a null _source
            assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode(), body);
            final Object source = ((JSONObject) new JSONParser().parse(body)).get("_source");
            assertTrue(source instanceof JSONObject, "no _source in " + body);
            final Object values = ((JSONObject) source).get(field);
            assertTrue(values instanceof JSONArray, "no list " + field + " in " + body);
            return new ArrayList<>((JSONArray) values);
        }
    }

    //The transaction hands the provider the complete indexed content of an existing element, and the provider uses it
    //as the upsert of every update it sends for the document. A document which turns out to be missing is then
    //recreated whole in the same round trip, whether the mutation removes content or not, instead of from the touched
    //fields alone or not at all
    @Test
    public void testACompleteDocumentRecreatesAMissingDocumentWhole() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", document(TIME, 1L, WEIGHT, 1.5, NAME, "whole"), true);
        clopen();
        tx.delete("vertex", "diverged", TIME, 1L, true);
        newTx();

        //Removing one field and adding another is the shape which cannot carry an upsert built from the additions
        tx.delete("vertex", "diverged", TIME, 1L, false);
        tx.add("vertex", "diverged", WEIGHT, 2.5, false);
        tx.registerCompleteDocument("vertex", "diverged",
            Arrays.asList(new IndexEntry(WEIGHT, 2.5), new IndexEntry(NAME, "whole")));
        clopen();

        assertEquals(ImmutableMap.of(WEIGHT, 2.5, NAME, "whole"), source("vertex", "diverged"));
        assertEquals(1, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(NAME, Cmp.EQUAL, "whole"))).count());
    }

    @Test
    public void testACompleteDocumentRecreatesAMissingDocumentForAnAdditionsOnlyMutation() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", document(TIME, 1L, NAME, "whole"), true);
        clopen();
        tx.delete("vertex", "diverged", TIME, 1L, true);
        newTx();

        //An addition alone used to recreate the document from itself, so the element was findable by the new field only
        tx.add("vertex", "diverged", WEIGHT, 2.5, false);
        tx.registerCompleteDocument("vertex", "diverged",
            Arrays.asList(new IndexEntry(TIME, 1L), new IndexEntry(NAME, "whole"), new IndexEntry(WEIGHT, 2.5)));
        clopen();

        assertEquals(ImmutableMap.of(TIME, 1L, NAME, "whole", WEIGHT, 2.5), source("vertex", "diverged"));
    }

    @Test
    public void testACompleteDocumentDoesNotDuplicateAListValueItRecreates() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", document(PHONE_LIST, "1"), true);
        clopen();
        tx.delete("vertex", "diverged", PHONE_LIST, "1", true);
        newTx();

        //Replacing a LIST value keeps both its deletion and its addition, which travel in the one script update. That
        //update inserts the complete document and its script is skipped, so neither is applied to the snapshot, which
        //already holds the outcome: the new value is there once and the old one is gone
        tx.delete("vertex", "diverged", PHONE_LIST, "1", false);
        tx.add("vertex", "diverged", PHONE_LIST, "2", false);
        tx.registerCompleteDocument("vertex", "diverged", Collections.singletonList(new IndexEntry(PHONE_LIST, "2")));
        clopen();

        assertEquals(ImmutableMap.of(PHONE_LIST, Arrays.asList("2")), source("vertex", "diverged"));
    }

    @Test
    public void testACompleteDocumentLeavesAnExistingDocumentToTheMutation() throws Exception {
        initialize("vertex");
        add("vertex", "diverged", document(TIME, 1L, NAME, "whole", PHONE_LIST, "1"), true);
        clopen();

        //The document exists, so the upsert is not used and the one script applies every change to it as the mutation
        //always has: a removal, a collection addition, and single valued fields both new and changed
        tx.delete("vertex", "diverged", TIME, 1L, false);
        tx.add("vertex", "diverged", PHONE_LIST, "2", false);
        tx.add("vertex", "diverged", WEIGHT, 2.5, false);
        tx.add("vertex", "diverged", NAME, "changed", false);
        tx.registerCompleteDocument("vertex", "diverged", Arrays.asList(new IndexEntry(NAME, "changed"),
            new IndexEntry(PHONE_LIST, "1"), new IndexEntry(PHONE_LIST, "2"), new IndexEntry(WEIGHT, 2.5)));
        clopen();

        assertEquals(ImmutableMap.of(NAME, "changed", PHONE_LIST, Arrays.asList("1", "2"), WEIGHT, 2.5),
            source("vertex", "diverged"));
    }

    @Test
    public void testACompleteDocumentKeepsADuplicateListValueItRecreates() throws Exception {
        initialize("vertex");
        //A LIST holds equal values apart, so the document starts with "x" twice
        tx.add("vertex", "diverged", PHONE_LIST, "x", true);
        tx.add("vertex", "diverged", PHONE_LIST, "x", true);
        clopen();
        tx.delete("vertex", "diverged", PHONE_LIST, "x", true);
        newTx();

        //Removing one of the two while changing another field: the snapshot already holds the remaining "x", and a
        //deletion applied on top of the inserted snapshot would take that one away as well
        tx.delete("vertex", "diverged", PHONE_LIST, "x", false);
        tx.add("vertex", "diverged", WEIGHT, 2.5, false);
        tx.registerCompleteDocument("vertex", "diverged",
            Arrays.asList(new IndexEntry(PHONE_LIST, "x"), new IndexEntry(WEIGHT, 2.5)));
        clopen();

        assertEquals(ImmutableMap.of(PHONE_LIST, Arrays.asList("x"), WEIGHT, 2.5), source("vertex", "diverged"));
    }

    @Test
    public void testACompleteDocumentRemovesOneOfTwoEqualListValuesFromAnExistingDocument() throws Exception {
        initialize("vertex");
        tx.add("vertex", "diverged", PHONE_LIST, "x", true);
        tx.add("vertex", "diverged", PHONE_LIST, "x", true);
        clopen();

        //The document exists, so the script runs against it and takes one occurrence away, as it always has
        tx.delete("vertex", "diverged", PHONE_LIST, "x", false);
        tx.add("vertex", "diverged", WEIGHT, 2.5, false);
        tx.registerCompleteDocument("vertex", "diverged",
            Arrays.asList(new IndexEntry(PHONE_LIST, "x"), new IndexEntry(WEIGHT, 2.5)));
        clopen();

        assertEquals(ImmutableMap.of(PHONE_LIST, Arrays.asList("x"), WEIGHT, 2.5), source("vertex", "diverged"));
    }

    //An upsert which recreates a missing document goes through the store's ingest pipeline like any new document:
    //Elasticsearch runs the pipeline of a bulk request on the upsert of an update whose document is missing. The
    //pipeline of the ingestvertex store sets a field which the complete document does not hold
    @Test
    public void testACompleteDocumentRecreatesAMissingDocumentThroughTheIngestPipeline() throws Exception {
        initialize("ingestvertex");
        add("ingestvertex", "diverged", document(TEXT, "bob"), true);
        clopen();
        tx.delete("ingestvertex", "diverged", TEXT, "bob", true);
        newTx();

        tx.add("ingestvertex", "diverged", WEIGHT, 2.5, false);
        tx.registerCompleteDocument("ingestvertex", "diverged",
            Arrays.asList(new IndexEntry(TEXT, "bob"), new IndexEntry(WEIGHT, 2.5)));
        clopen();

        assertEquals(ImmutableMap.of(TEXT, "bob", WEIGHT, 2.5, STRING, "hello"), source("ingestvertex", "diverged"));
    }

    //The complete document only matters when the document turns out to be missing. An update which it would make larger
    //than bulk-chunk-size-limit-bytes is sent without it and updates the existing document exactly as before, instead
    //of failing an update which fits on its own
    @Test
    public void testACompleteDocumentTooLargeToSendIsLeftOut() throws Exception {
        initialize("vertex");
        final String large = RandomStringUtils.randomAlphanumeric(5000);
        add("vertex", "large", document(TEXT, large), true);
        clopen();

        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index.es.elasticsearch.bulk-chunk-size-limit-bytes", "2000");
        final ElasticSearchIndex smallChunks = new ElasticSearchIndex(makeESTestConfig("es", cc));
        try {
            final IndexTransaction update = new IndexTransaction(smallChunks, indexRetriever,
                StandardBaseTransactionConfig.of(TimestampProviders.MILLI), Duration.ofSeconds(5));
            update.add("vertex", "large", WEIGHT, 2.5, false);
            update.registerCompleteDocument("vertex", "large",
                Arrays.asList(new IndexEntry(TEXT, large), new IndexEntry(WEIGHT, 2.5)));
            update.commit();
        } finally {
            smallChunks.close();
        }

        assertEquals(ImmutableMap.of(TEXT, large, WEIGHT, 2.5), source("vertex", "large"));
    }

    //Up to Elasticsearch 6 a document is addressed through its mapping type, which JanusGraph names after the store
    private static String documentPath(String store, String documentId) {
        final String type = JanusGraphElasticsearchContainer.getEsMajorVersion().value <= 6 ? store : "_doc";
        return INDEX_NAME.getDefaultValue() + "_" + store + "/" + type + "/" + documentId;
    }

    private Map<String, Object> source(String store, String documentId) throws Exception {
        final HttpGet get = new HttpGet(documentPath(store, documentId));
        try (CloseableHttpResponse response = httpClient.execute(host, get)) {
            final String body = EntityUtils.toString(response.getEntity());
            //A missing document is a failure of the test, said as such rather than as a cast of a null _source
            assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode(), body);
            final Map<?, ?> source = (Map<?, ?>) ((JSONObject) new JSONParser().parse(body)).get("_source");
            assertNotNull(source, "no _source in " + body);
            final Map<String, Object> copy = new HashMap<>();
            source.forEach((key, value) -> copy.put(String.valueOf(key), value));
            return copy;
        }
    }

    private static Multimap<String, Object> document(Object... keysAndValues) {
        assertEquals(0, keysAndValues.length % 2, "keys and values come in pairs");
        final Multimap<String, Object> document = HashMultimap.create();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            document.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return document;
    }

    private static Multimap<String, Object> documentWith(String key, Object value) {
        final Multimap<String, Object> document = HashMultimap.create();
        document.put(key, value);
        return document;
    }

    @Test
    public void testUnescapedDollarInSet() throws Exception {
        initialize("vertex");

        Multimap<String, Object> initialDoc = HashMultimap.create();
        initialDoc.put(PHONE_SET, "12345");

        add("vertex", "unescaped", initialDoc, true);

        clopen();

        Multimap<String, Object> updateDoc = HashMultimap.create();
        updateDoc.put(PHONE_SET, "$123");
        add("vertex", "unescaped", updateDoc, false);

        add("vertex", "other", getRandomDocument(), true);

        clopen();

        assertEquals("unescaped", tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "$123"))).toArray()[0]);
        assertEquals("unescaped", tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "12345"))).toArray()[0]);
    }

    /**
     * Test adding and overwriting with long string content.
     *
     */
    @Test
    public void testUpdateAdditionWithLongString() throws Exception {
        initialize("vertex");
        Multimap<String, Object> initialDoc = HashMultimap.create();
        initialDoc.put(TEXT, RandomStringUtils.randomAlphanumeric(500000) + " bob " + RandomStringUtils.randomAlphanumeric(500000));

        add("vertex", "long", initialDoc, true);

        clopen();

        assertEquals(1, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).count());
        assertEquals(0, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "world"))).count());

        tx.add("vertex", "long", TEXT, RandomStringUtils.randomAlphanumeric(500000) + " world " + RandomStringUtils.randomAlphanumeric(500000), false);

        clopen();

        assertEquals(0, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "world"))).count());
    }

    /**
     * Test ingest pipeline.
     */
    @Test
    public void testIngestPipeline() throws Exception {
        initialize("ingestvertex");
        final Multimap<String, Object> docs = HashMultimap.create();
        docs.put(TEXT, "bob");
        add("ingestvertex", "pipeline", docs, true);
        clopen();
        assertEquals(1, tx.queryStream(new IndexQuery("ingestvertex", PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).count());
        assertEquals(1, tx.queryStream(new IndexQuery("ingestvertex", PredicateCondition.of(STRING, Cmp.EQUAL, "hello"))).count());
    }

    @Test
    public void testMapKey2Field_IllegalCharacter() {
        assertThrows(IllegalArgumentException.class, () -> {
            index.mapKey2Field("here is an illegal character: " + REPLACEMENT_CHAR, null);
        });
    }

    @Test
    public void testMapKey2Field_MappingSpaces() {
        String expected = "field" + REPLACEMENT_CHAR + "name" + REPLACEMENT_CHAR + "with" + REPLACEMENT_CHAR + "spaces";
        assertEquals(expected, index.mapKey2Field("field name with spaces", null));
    }

    @Test
    public void testClearStorageWithAliases() throws Exception {
        IOUtils.closeQuietly(httpClient.execute(host, new HttpPut("test1")));
        IOUtils.closeQuietly(httpClient.execute(host, new HttpPut("test2")));
        final HttpPost addAlias = new HttpPost("_aliases");
        addAlias.setHeader("Content-Type", "application/json");
        addAlias.setEntity(new StringEntity("{\"actions\": [{\"add\": {\"indices\": [\"test1\", \"test2\"], \"alias\": \"alias1\"}}]}", StandardCharsets.UTF_8));
        IOUtils.closeQuietly(httpClient.execute(host, addAlias));

        initialize("vertex");
        assertTrue(indexExists(GraphDatabaseConfiguration.INDEX_NAME.getDefaultValue()));

        index.clearStorage();

        assertFalse(indexExists(GraphDatabaseConfiguration.INDEX_NAME.getDefaultValue()));
        assertTrue(indexExists("test1"));
        assertTrue(indexExists("test2"));
    }

    @Test
    public void testClearStoreOfAMixedCaseStoreName() throws Exception {
        //A mixed index stores its JanusGraph index name as the store name verbatim and those names are case
        //sensitive, while an Elasticsearch index name is always lowercase. clearStore has to derive the name the
        //same way every read and write path does, or SchemaAction.DISCARD_INDEX targets a name which cannot exist,
        //the documents survive, and the schema is marked DISCARDED anyway
        final String storeName = "vertexByName";
        //Locale.ROOT so the expectation cannot drift with the default locale of whoever runs this. The store name is
        //ASCII and carries no dotted I, so it is the same string the production derivation produces in any locale
        final String indexStoreName = INDEX_NAME.getDefaultValue() + "_" + storeName.toLowerCase(Locale.ROOT);

        initialize(storeName);
        assertTrue(indexExists(indexStoreName));

        index.clearStore(storeName);

        assertFalse(indexExists(indexStoreName));
    }

    @Test
    public void testCustomMappingProperty() throws BackendException, IOException, ParseException, URISyntaxException {

        String mappingTypeName = "vertex";
        String indexPrefix = "janusgraph";
        String parameterName = "store";
        Boolean parameterValue = true;

        String field = "field_with_custom_prop";

        KeyInformation keyInfo = new StandardKeyInformation(
            String.class,
            Cardinality.SINGLE,
            Mapping.STRING.asParameter(),
            Parameter.of(ParameterType.customParameterName(parameterName), parameterValue));

        index.register(mappingTypeName, field, keyInfo, tx);

        String indexName = indexPrefix+"_"+mappingTypeName;

        CloseableHttpResponse response = getESMapping(indexName, mappingTypeName);

        // Fallback to multitype index
        if(response.getStatusLine().getStatusCode() != 200){
            indexName = indexPrefix;
            response = getESMapping(indexName, mappingTypeName);
        }

        HttpEntity entity = response.getEntity();

        JSONObject json = (JSONObject) new JSONParser().parse(EntityUtils.toString(entity));

        String returnedProperty;

        if(JanusGraphElasticsearchContainer.getEsMajorVersion().value < 7){
            returnedProperty = retrieveValueFromJSON(json,
                indexName, "mappings", mappingTypeName, "properties", field, parameterName);
        } else {
            returnedProperty = retrieveValueFromJSON(json,
                indexName, "mappings", "properties", field, parameterName);
        }

        assertEquals(parameterValue.toString(), returnedProperty);

        IOUtils.closeQuietly(response);
    }

    public static Stream<String> cardinalityTestCollectionNameParams() {
        return Stream.of(PHONE_SET, PHONE_LIST);
    }

    @ParameterizedTest
    @MethodSource("cardinalityTestCollectionNameParams")
    public void testCollectionCardinality(String collectionName) throws Exception {
        initialize("vertex");

        Multimap<String, Object> initialDoc = HashMultimap.create();
        initialDoc.put(collectionName, "12345");

        add("vertex", "test", initialDoc, true);

        clopen();

        Multimap<String, Object> updateDoc = HashMultimap.create();
        updateDoc.put(collectionName, "123456");

        add("vertex", "test", updateDoc, false);

        clopen();

        add("vertex", "test", initialDoc, false);

        clopen();

        tx.delete("vertex", "test", collectionName, "12345", false);

        clopen();

        assertEquals("test", tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(collectionName, Cmp.EQUAL, "123456"))).toArray()[0]);

        if(PHONE_SET.equals(collectionName)){
            assertEquals(0, tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "12345"))).count());
        } else {
            assertEquals("test", tx.queryStream(new IndexQuery("vertex", PredicateCondition.of(PHONE_LIST, Cmp.EQUAL, "12345"))).toArray()[0]);
        }
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

    @Test
    public void testShouldNotShareIndexStoreNameCacheBetweenElasticSearchIndexInstances() throws BackendException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final String index1 = "es1";
        final String index2 = "es2";

        final CommonsConfiguration cc1 = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc1.set("index." + index1 + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        cc1.set("index." + index1 + ".elasticsearch.enable_index_names_cache", true);

        final CommonsConfiguration cc2 = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc1.set("index." + index2 + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        cc1.set("index." + index2 + ".elasticsearch.enable_index_names_cache", true);

        Configuration configuration1 = makeESTestConfig(index1, cc1);
        Configuration configuration2 = makeESTestConfig(index2, cc2);

        ElasticSearchIndex instance1 = new ElasticSearchIndex(configuration1);
        ElasticSearchIndex instance2 = new ElasticSearchIndex(configuration2);

        String indexName1 = configuration1.get(INDEX_NAME);
        String indexName2 = configuration2.get(INDEX_NAME);

        Map<String, String> indexStoreNamesCache1 = (Map<String, String>) FieldUtils.readField(instance1, "indexStoreNamesCache", true);
        Map<String, String> indexStoreNamesCache2 = (Map<String, String>) FieldUtils.readField(instance2, "indexStoreNamesCache", true);

        Method method1 = instance1.getClass().getDeclaredMethod("getIndexStoreName", String.class);
        method1.setAccessible(true);

        Method method2 = instance1.getClass().getDeclaredMethod("getIndexStoreName", String.class);
        method2.setAccessible(true);

        String store = "Test_store";

        method1.invoke(instance1, store);
        assertEquals(1, indexStoreNamesCache1.size());
        assertEquals(0, indexStoreNamesCache2.size());

        method2.invoke(instance2, store);
        assertEquals(1, indexStoreNamesCache1.size());
        assertEquals(1, indexStoreNamesCache2.size());

        assertEquals(indexName1 + ElasticSearchIndex.INDEX_NAME_SEPARATOR + store.toLowerCase(), indexStoreNamesCache1.get(store));
        assertEquals(indexName2 + ElasticSearchIndex.INDEX_NAME_SEPARATOR + store.toLowerCase(), indexStoreNamesCache2.get(store));
    }

    @Test
    public void testShouldNotUseIndexStoreNameCache() throws BackendException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final String index = "es1";

        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index." + index + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        cc.set("index." + index + ".elasticsearch.enable_index_names_cache", false);

        ElasticSearchIndex instance = new ElasticSearchIndex(makeESTestConfig(index, cc));

        Map<String, String> indexStoreNamesCache = (Map<String, String>) FieldUtils.readField(instance, "indexStoreNamesCache", true);

        Method method = instance.getClass().getDeclaredMethod("getIndexStoreName", String.class);
        method.setAccessible(true);

        String store = "Test_store";

        method.invoke(instance, store);
        assertEquals(0, indexStoreNamesCache.size());
    }

    private CloseableHttpResponse getESMapping(String indexName, String mappingTypeName) throws IOException, URISyntaxException {

        URIBuilder uriBuilder;

        if(JanusGraphElasticsearchContainer.getEsMajorVersion().value < 7){
            uriBuilder = new URIBuilder(indexName+"/_mapping/"+mappingTypeName);
        } else {
            uriBuilder = new URIBuilder(indexName+"/_mapping");
        }

        final HttpGet httpGet = new HttpGet(uriBuilder.build());
        return httpClient.execute(host, httpGet);
    }

    private String retrieveValueFromJSON(JSONObject json, String ... hierarchy){

        for(int i=0; i<hierarchy.length; i++){
            if(i+1==hierarchy.length){
                return json.get(hierarchy[i]).toString();
            }
            json = (JSONObject) json.get(hierarchy[i]);
        }

        return null;
    }

    private boolean indexExists(String name) throws IOException {
        final CloseableHttpResponse response = httpClient.execute(host, new HttpHead(name));
        final boolean exists = response.getStatusLine().getStatusCode() == 200;
        IOUtils.closeQuietly(response);
        return exists;
    }
}
