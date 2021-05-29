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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.es.mapping.TypedIndexMappings;
import org.janusgraph.diskstorage.es.mapping.TypelessIndexMappings;
import org.janusgraph.diskstorage.es.rest.RestElasticSearchClient;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.ALLOW_MAPPING_UPDATE;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.USE_EXTERNAL_MAPPINGS;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.USE_MAPPING_FOR_ES7;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test behavior JanusGraph ConfigOptions governing ES client setup.
 */
@Testcontainers
public class ElasticsearchConfigTest {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchConfigTest.class);
    @Container
    public static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();

    private static final String INDEX_NAME = "escfg";

    private static final String ANALYZER_KEYWORD = "keyword";

    private static final String ANALYZER_ENGLISH = "english";

    private static final String ANALYZER_STANDARD = "standard";

    private HttpHost host;

    private CloseableHttpClient httpClient;

    private ObjectMapper objectMapper;

    public static Stream<Boolean> useMappingsForES7Configuration() {
        return Stream.of(true, false);
    }

    @BeforeEach
    public void setup() throws Exception {
        httpClient = HttpClients.createDefault();
        host = new HttpHost(InetAddress.getByName(esr.getHostname()), esr.getPort());
        objectMapper = new ObjectMapper();
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("_template/template_1")));
    }

    @AfterEach
    public void teardown() throws Exception {
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("janusgraph*")));
        IOUtils.closeQuietly(httpClient);
    }

    @Test
    public void testJanusGraphFactoryBuilder() {
        final JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "inmemory");
        builder.set("index." + INDEX_NAME + ".hostname", esr.getHostname() + ":" + esr.getPort());
        final JanusGraph graph = builder.open(); // Must not throw an exception
        assertTrue(graph.isOpen());
        graph.close();
    }

    @Test
    public void testClientThrowsExceptionIfServerNotReachable() throws BackendException, InterruptedException {
        final ModifiableConfiguration config = esr.setConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        config.set(INDEX_HOSTS, new String[]{ "localhost:" + esr.getPort()+1 }, INDEX_NAME);
        final Configuration wrongHostConfig = config.restrictTo(INDEX_NAME);

        assertThrows(Exception.class, () -> new ElasticSearchIndex(wrongHostConfig));
    }

    @ParameterizedTest
    @MethodSource("useMappingsForES7Configuration")
    public void testIndexCreationOptions(Boolean useMappingsForES7) throws InterruptedException, BackendException, IOException {
        final int shards = 7;

        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.create.ext.number_of_shards", String.valueOf(shards));
        if(useMappingsForES7){
            cc.set("index." + INDEX_NAME + ".elasticsearch.use-mapping-for-es7", String.valueOf(true));
        }
        final ModifiableConfiguration config =
            new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                cc, BasicConfiguration.Restriction.NONE);
        esr.setConfiguration(config, INDEX_NAME);
        final Configuration indexConfig = config.restrictTo(INDEX_NAME);
        final IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        final ElasticSearchClient client = ElasticSearchSetup.REST_CLIENT.connect(indexConfig).getClient();

        assertEquals(String.valueOf(shards), client.getIndexSettings("janusgraph_jvmlocal_test_store").get("number_of_shards"));

        client.close();
    }

    @ParameterizedTest
    @MethodSource("useMappingsForES7Configuration")
    public void testExternalMappingsViaMapping(Boolean useMappingsForES7) throws Exception {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";
        final ModifiableConfiguration modifiableConfiguration = esr
            .setConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME)
            .set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME);

        if(useMappingsForES7){
            modifiableConfiguration.set(USE_MAPPING_FOR_ES7, true, INDEX_NAME);
        }

        final Configuration indexConfig = modifiableConfiguration.restrictTo(INDEX_NAME);

        final IndexProvider idx = open(indexConfig);

        // Test that the "date" property throws an exception.
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
            .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        final IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        try {
            idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
            fail("should fail");
        } catch (final PermanentBackendException e) {
            log.debug(e.getMessage(), e);
        }

        if(isMappingUsed(idx)){
            executeRequestWithStringEntity(idx, "janusgraph_"+storeName, readTypesMapping("/strict_mapping.json"));
        } else {
            executeRequestWithStringEntity(idx, "janusgraph_"+storeName, readTypelessMapping("/typeless_strict_mapping.json"));
        }

        // Test that the "date" property works well.
        idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
        // Test that the "weight" property throws an exception.
        try {
            idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
            fail("should fail");
        } catch (final BackendException e) {
            log.debug(e.getMessage(), e);
        }
        itx.rollback();
        idx.close();
    }

    private TypedIndexMappings readTypesMapping(final String mappingFilePath) throws IOException {
        try (final InputStream inputStream = getClass().getResourceAsStream(mappingFilePath)) {
            return objectMapper.readValue(inputStream, new TypeReference<TypedIndexMappings>() {});
        }
    }

    private TypelessIndexMappings readTypelessMapping(final String mappingFilePath) throws IOException {
        try (final InputStream inputStream = getClass().getResourceAsStream(mappingFilePath)) {
            return objectMapper.readValue(inputStream, new TypeReference<TypelessIndexMappings>() {});
        }
    }

    @ParameterizedTest
    @MethodSource("useMappingsForES7Configuration")
    public void testExternalDynamic(Boolean useMappingsForES7) throws Exception {

        testExternalDynamic(false, useMappingsForES7);
    }

    @ParameterizedTest
    @MethodSource("useMappingsForES7Configuration")
    public void testUpdateExternalDynamicMapping(Boolean useMappingsForES7) throws Exception {

        testExternalDynamic(true, useMappingsForES7);
    }

    @ParameterizedTest
    @MethodSource("useMappingsForES7Configuration")
    public void testExternalMappingsViaTemplate(Boolean useMappingsForES7) throws Exception {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";
        final ModifiableConfiguration modifiableConfiguration = esr
            .setConfiguration(GraphDatabaseConfiguration.buildGraphConfiguration(), INDEX_NAME)
            .set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME);

        if(useMappingsForES7){
            modifiableConfiguration.set(USE_MAPPING_FOR_ES7, true, INDEX_NAME);
        }

        final Configuration indexConfig = modifiableConfiguration.restrictTo(INDEX_NAME);

        final IndexProvider idx = open(indexConfig);

        final Map<String, Object> content = new HashMap<>(2);
        content.put("template", "janusgraph_test_mapping*");

        if(isMappingUsed(idx)){
            content.put("mappings", readTypesMapping("/strict_mapping.json").getMappings());
        } else {
            content.put("mappings", readTypelessMapping("/typeless_strict_mapping.json").getMappings());
        }

        executeRequestWithStringEntity(idx, "_template/template_1", content);

        final HttpPut newMapping = new HttpPut("janusgraph_" + storeName);
        executeRequest(newMapping);

        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
            .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        final IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);

        // Test that the "date" property works well.
        idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
        // Test that the "weight" property throws an exception.
        try {
            idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
            fail("should fail");
        } catch (final BackendException e) {
            log.debug(e.getMessage(), e);
        }
        itx.rollback();
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
            if(statusCode < 200 || statusCode >= 300 || EntityUtils.toString(res.getEntity()).contains("error")){
                fail("Failed to execute a request:"+request.toString()+". Entity: "+EntityUtils.toString(res.getEntity()));
            }
        }
    }

    private IndexProvider open(Configuration indexConfig) throws BackendException {
        final ElasticSearchIndex idx = new ElasticSearchIndex(indexConfig);
        idx.clearStorage();
        idx.close();
        return new ElasticSearchIndex(indexConfig);
    }

    private void testExternalDynamic(boolean withUpdateMapping, boolean useMappingsForES7) throws Exception {

        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";

        final Configuration indexConfig = buildIndexConfigurationForExternalDynamic(withUpdateMapping, useMappingsForES7);

        final IndexProvider idx = open(indexConfig);

        // Test that the "date" property throws an exception.
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
            .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
        final BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        final IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        try {
            idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
            fail("should fail");
        } catch (final PermanentBackendException e) {
            log.debug(e.getMessage(), e);
        }

        if(isMappingUsed(idx)){
            executeRequestWithStringEntity(idx, "janusgraph_"+storeName, readTypesMapping("/dynamic_mapping.json"));
        } else {
            executeRequestWithStringEntity(idx, "janusgraph_"+storeName, readTypelessMapping("/typeless_dynamic_mapping.json"));
        }

        // Test that the "date" property works well.
        idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
        // Test that the "weight" property works well due to dynamic mapping.
        idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
        itx.rollback();
        idx.close();
        final ElasticSearchClient client = ElasticSearchSetup.REST_CLIENT.connect(indexConfig).getClient();
        final Map<String, Object> properties = client.getMapping("janusgraph_"+storeName, storeName).getProperties();

        assertEquals(withUpdateMapping, properties.containsKey("weight"), properties.toString());
    }

    private Configuration buildIndexConfigurationForExternalDynamic(boolean withUpdateMapping, boolean useMappingsForES7){

        ModifiableConfiguration indexConfig = GraphDatabaseConfiguration.buildGraphConfiguration().set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME);
        indexConfig = indexConfig.set(INDEX_PORT, esr.getPort(), INDEX_NAME);
        if(withUpdateMapping){
            indexConfig = indexConfig.set(ALLOW_MAPPING_UPDATE, true, INDEX_NAME);
        }
        if(useMappingsForES7){
            indexConfig.set(USE_MAPPING_FOR_ES7, true, INDEX_NAME);
        }
        return indexConfig.restrictTo(INDEX_NAME);
    }

    private void executeRequestWithStringEntity(IndexProvider idx, String endpoint, Object content) throws URISyntaxException, IOException {

        ElasticSearchIndex elasticSearchIndex = ((ElasticSearchIndex) idx);

        URIBuilder uriBuilder = new URIBuilder(endpoint);
        if(ElasticMajorVersion.SEVEN.equals(elasticSearchIndex.getVersion()) && elasticSearchIndex.isUseMappingForES7()){
            uriBuilder.setParameter(RestElasticSearchClient.INCLUDE_TYPE_NAME_PARAMETER, "true");
        }

        final HttpPut newMapping = new HttpPut(uriBuilder.build());
        newMapping.setEntity(new StringEntity(objectMapper.writeValueAsString(content), StandardCharsets.UTF_8));
        executeRequest(newMapping);
    }

    private boolean isMappingUsed(IndexProvider idx){
        ElasticSearchIndex elasticSearchIndex = ((ElasticSearchIndex) idx);

        return elasticSearchIndex.getVersion().getValue() < 7 ||
            (ElasticMajorVersion.SEVEN.equals(elasticSearchIndex.getVersion()) && elasticSearchIndex.isUseMappingForES7());
    }
}
