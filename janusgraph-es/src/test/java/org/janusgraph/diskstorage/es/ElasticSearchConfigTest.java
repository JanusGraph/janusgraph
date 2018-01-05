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

import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.es.IndexMappings;
import org.janusgraph.diskstorage.indexing.*;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.*;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;

import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

import static org.junit.Assert.*;

/**
 * Test behavior JanusGraph ConfigOptions governing ES client setup.
 */
public class ElasticSearchConfigTest {

    private static final String INDEX_NAME = "escfg";

    private static final String ANALYZER_KEYWORD = "keyword";

    private static final String ANALYZER_ENGLISH = "english";

    private static final String ANALYZER_STANDARD = "standard";

    private ElasticsearchRunner esr;

    private HttpHost host;

    private CloseableHttpClient httpClient;

    private ObjectMapper objectMapper;

    @Before
    public void setup() throws Exception {
        esr = new ElasticsearchRunner();
        esr.start();

        httpClient = HttpClients.createDefault();
        host = new HttpHost(InetAddress.getByName(esr.getHostname()), ElasticsearchRunner.PORT);

        objectMapper = new ObjectMapper();
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("_template/template_1")));
    }

    @After
    public void teardown() throws Exception {
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("janusgraph*")));
        IOUtils.closeQuietly(httpClient);
        esr.stop();
    }

    @Test
    public void testJanusGraphFactoryBuilder() {
        final JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "inmemory");
        builder.set("index." + INDEX_NAME + ".elasticsearch.hostname", esr.getHostname() + ":" + ElasticsearchRunner.PORT);
        final JanusGraph graph = builder.open(); // Must not throw an exception
        assertTrue(graph.isOpen());
        graph.close();
    }

    @Test
    public void testClient() throws BackendException, InterruptedException {
        final ModifiableConfiguration config = esr.setElasticsearchConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        config.set(INDEX_HOSTS, new String[]{ "10.11.12.13:" + ElasticsearchRunner.PORT }, INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);
        Throwable failure = null;
        try {
            new ElasticSearchIndex(indexConfig);
        } catch (final Throwable t) {
            failure = t;
        }
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);
    }

    @Test
    public void testIndexCreationOptions() throws InterruptedException, BackendException, IOException {
        final int shards = 7;

        final CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.create.ext.number_of_shards", String.valueOf(shards));
        final ModifiableConfiguration config =
            new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                cc, BasicConfiguration.Restriction.NONE);
        esr.setElasticsearchConfiguration(config, INDEX_NAME);
        final Configuration indexConfig = config.restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        final ElasticSearchClient client = ElasticSearchSetup.REST_CLIENT.connect(indexConfig).getClient();

        assertEquals(String.valueOf(shards), client.getIndexSettings("janusgraph_jvmlocal_test_store").get("number_of_shards"));

        client.close();
    }

    @Test
    public void testExternalMappingsViaMapping() throws Exception {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";
        final Configuration indexConfig = GraphDatabaseConfiguration.buildGraphConfiguration().set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME).restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        final ElasticMajorVersion version = ((ElasticSearchIndex) idx).getVersion();

        //Test create index KO mapping is not push
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
            .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        final IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        try {
            idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
            fail("should fail");
        } catch (final PermanentBackendException ignored) {
        }

        final HttpPut newMapping = new HttpPut("janusgraph_"+storeName);
        newMapping.setEntity(new StringEntity(objectMapper.writeValueAsString(readMapping(version, "/strict_mapping.json")), Charset.forName("UTF-8")));
        executeRequest(newMapping);

        // Test date property OK
        idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
        // Test weight property KO
        try {
            idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
            fail("should fail");
        } catch (final BackendException ignored) {
        }
        itx.rollback();
        idx.close();
    }

    private IndexMappings readMapping(final ElasticMajorVersion version, final String mappingFilePath) throws IOException {
        try (final InputStream inputStream = getClass().getResourceAsStream(mappingFilePath)) {
            final IndexMappings mappings = objectMapper.readValue(inputStream, new TypeReference<IndexMappings>() {});
            if (version.getValue() < 5) {
                // downgrade from text to string and keyword to string/not-analyzed
                mappings.getMappings().values().stream()
                    .flatMap(mapping -> mapping.getProperties().entrySet().stream())
                    .map(entry -> (Map<String, Object>) entry.getValue())
                    .forEach(properties -> {
                        if (properties.get("type").equals("keyword")) {
                            properties.put("type", "string");
                            properties.put("index", "not_analyzed");
                        } else if (properties.get("type").equals("text")) {
                            properties.put("type", "string");
                        }
                    });
            }
            return mappings;
        }
    }

    @Test
    public void testExternalDynamic() throws Exception {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";
        final Configuration indexConfig = GraphDatabaseConfiguration.buildGraphConfiguration().set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME).restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        final ElasticMajorVersion version = ((ElasticSearchIndex) idx).getVersion();

        //Test create index KO mapping is not push
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
            .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        final IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        try {
            idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
            fail("should fail");
        } catch (final PermanentBackendException ignored) {
        }

        final HttpPut newMapping = new HttpPut("janusgraph_"+storeName);
        newMapping.setEntity(new StringEntity(objectMapper.writeValueAsString(readMapping(version, "/dynamic_mapping.json")), Charset.forName("UTF-8")));
        executeRequest(newMapping);

        // Test date property OK
        idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
        // Test weight property OK  because dynamic mapping
        idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
        itx.rollback();
        idx.close();
    }

    @Test
    public void testExternalMappingsViaTemplate() throws Exception {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";
        final Configuration indexConfig = GraphDatabaseConfiguration.buildGraphConfiguration().set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME).restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        final ElasticMajorVersion version = ((ElasticSearchIndex) idx).getVersion();

        final HttpPut newTemplate = new HttpPut("_template/template_1");
        final Map<String, Object> content = ImmutableMap.of("template", "janusgraph_test_mapping*", "mappings", readMapping(version, "/strict_mapping.json").getMappings());
        newTemplate.setEntity(new StringEntity(objectMapper.writeValueAsString(content), Charset.forName("UTF-8")));
        executeRequest(newTemplate);
        final HttpPut newMapping = new HttpPut("janusgraph_" + storeName);
        executeRequest(newMapping);

        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
            .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        final IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);

        // Test date property OK
        idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
        // Test weight property KO
        try {
            idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
            fail("should fail");
        } catch (final BackendException ignored) {
        }
        itx.rollback();
        idx.close();
    }

    @Test
    public void testSplitIndexToMultiType() throws InterruptedException, BackendException, IOException {
        final ModifiableConfiguration config = esr.setElasticsearchConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME);
        config.set(USE_DEPRECATED_MULTITYPE_INDEX, false, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);

        try {
            config.set(USE_DEPRECATED_MULTITYPE_INDEX, true, INDEX_NAME);
            indexConfig = config.restrictTo(INDEX_NAME);
            open(indexConfig);
            fail("should fail");
        } catch (final IllegalArgumentException ignored) {
        }
        idx.close();
    }

    @Test
    public void testMultiTypeToSplitIndex() throws InterruptedException, BackendException, IOException {
        final ModifiableConfiguration config = esr.setElasticsearchConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME);
        config.set(USE_DEPRECATED_MULTITYPE_INDEX, true, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);

        try {
            config.set(USE_DEPRECATED_MULTITYPE_INDEX, false, INDEX_NAME);
            indexConfig = config.restrictTo(INDEX_NAME);
            open(indexConfig);
            fail("should fail");
        } catch (final IllegalArgumentException ignored) {
        }
        idx.close();
    }

    @Test
    public void testMultiTypeUpgrade() throws InterruptedException, BackendException, IOException {
        // create multi-type index
        final ModifiableConfiguration config = esr.setElasticsearchConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME);
        config.set(USE_DEPRECATED_MULTITYPE_INDEX, true, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        // should be able to open multi-type index if USE_DEPRECATED_MULTITYPE_INDEX is unset
        config.remove(USE_DEPRECATED_MULTITYPE_INDEX, INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);
        idx = open(indexConfig);
        idx.close();
    }

    private void simpleWriteAndQuery(IndexProvider idx) throws BackendException, InterruptedException {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "jvmlocal_test_store";
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest.getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_STANDARD, ANALYZER_KEYWORD));

        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        for (final Entry<String, KeyInformation> entry : IndexProviderTest.getMapping(idx.getFeatures(), "english", "keyword").entrySet()) {
           idx.register(storeName, entry.getKey(), entry.getValue(), itx);
        }
        assertEquals(0, itx.queryStream(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "ali"))).count());
        itx.add(storeName, "doc", IndexProviderTest.NAME, "alice", false);
        itx.commit();
        Thread.sleep(1500L); // Slightly longer than default 1s index.refresh_interval
        itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        assertEquals(0, itx.queryStream(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "zed"))).count());
        assertEquals(1, itx.queryStream(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "ali"))).count());
        itx.rollback();
    }

    private void executeRequest(HttpRequestBase request) throws IOException {
        request.setHeader("Content-Type", "application/json");
        try (final CloseableHttpResponse res = httpClient.execute(host, request)) {
            final int statusCode = res.getStatusLine().getStatusCode();
            assertTrue("request failed", statusCode >= 200 && statusCode < 300);
            assertFalse(EntityUtils.toString(res.getEntity()).contains("error"));
        }
    }

    private IndexProvider open(Configuration indexConfig) throws BackendException {
        final ElasticSearchIndex idx = new ElasticSearchIndex(indexConfig);
        idx.clearStorage();
        idx.close();
        return new ElasticSearchIndex(indexConfig);
    }

}
