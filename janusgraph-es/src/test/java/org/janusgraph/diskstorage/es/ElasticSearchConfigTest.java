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

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.Duration;

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

    private static final int PORT = 9200;

    private ElasticsearchRunner esr;

    private HttpHost host;

    private CloseableHttpClient httpClient;

    @Before
    public void setup() throws Exception {
        if (!ElasticsearchRunner.IS_EXTERNAL) {
            esr = new ElasticsearchRunner();
            esr.start();
            Thread.sleep(5000);
        }

        httpClient = HttpClients.createDefault();
        try {
            host = new HttpHost(InetAddress.getByName("127.0.0.1"), 9200);
        } catch (UnknownHostException e) {
            fail(e.getMessage());
        }
    }

    @After
    public void teardown() throws Exception {
        if (!ElasticsearchRunner.IS_EXTERNAL) {
            esr.stop();
        }
        IOUtils.closeQuietly(httpClient);
    }

    @Test
    public void testJanusGraphFactoryBuilder() {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "inmemory");
        builder.set("index." + INDEX_NAME + ".elasticsearch.hostname", "127.0.0.1:" + PORT);
        JanusGraph graph = builder.open(); // Must not throw an exception
        assertTrue(graph.isOpen());
        graph.close();
    }

    @Test
    public void testClient() throws BackendException, InterruptedException {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1:" + PORT }, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);
        idx.close();

        config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "10.11.12.13:" + PORT }, INDEX_NAME);
        indexConfig = config.restrictTo(INDEX_NAME);
        Throwable failure = null;
        try {
            idx = new ElasticSearchIndex(indexConfig);
        } catch (Throwable t) {
            failure = t;
        }
        // idx.close();
        Assert.assertNotNull("ES client failed to throw exception on connection failure", failure);
    }

    @Test
    public void testIndexCreationOptions() throws InterruptedException, BackendException, IOException {
        final int shards = 7;

        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + INDEX_NAME + ".elasticsearch.create.ext.number_of_shards", String.valueOf(shards));
        ModifiableConfiguration config =
            new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                cc, BasicConfiguration.Restriction.NONE);
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), INDEX_NAME);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1:" + PORT }, INDEX_NAME);
        config.set(GraphDatabaseConfiguration.INDEX_NAME, "janusgraph_creation_opts", INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        IndexProvider idx = open(indexConfig);
        simpleWriteAndQuery(idx);

        ElasticSearchClient client = ElasticSearchSetup.REST_CLIENT.connect(indexConfig).getClient();

        assertEquals(String.valueOf(shards), client.getIndexSettings("janusgraph_creation_opts").get("number_of_shards"));

        idx.close();
        client.close();
    }

    @Test
    public void testExternalMappingsViaMapping() throws BackendException {
        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "test_mapping";
        CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        ModifiableConfiguration config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, cc,
                BasicConfiguration.Restriction.NONE);
        config.set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME);
        Configuration indexConfig = config.restrictTo(INDEX_NAME);
        FileInputStream fis = null;
        try (CloseableHttpResponse res = httpClient.execute(host, new HttpDelete("janusgraph"))) {} catch (Exception e) {}
        try {
            //Test create index KO mapping is not push
            try {
                new ElasticSearchIndex(indexConfig);
                fail("should failed");
            } catch (IllegalArgumentException e) {
            }
            HttpPut newMapping = new HttpPut("janusgraph");
            fis = new FileInputStream(new File("src/test/resources/mapping.json"));
            newMapping.setEntity(new StringEntity(Joiner.on("").join(IOUtils.readLines(fis)), Charset.forName("UTF-8")));
            executeRequest(newMapping);

            IndexProvider idx = new ElasticSearchIndex(indexConfig);
            final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
                    .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
            BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
            IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);

            // Test date property OK
            idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
            // Test weight property KO
            try {
                idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
                fail("should failed");
            } catch (BackendException e) {
            }
        } catch (IOException e) {
            fail(e.getMessage());
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    @Test
    public void testExternalMappingsViaTemplate() throws BackendException {
        try (CloseableHttpResponse res = httpClient.execute(host, new HttpDelete("janusgraph"))) {} catch (Exception e) {}
        FileInputStream fis = null;
        try {
            HttpPut newTemplate = new HttpPut("_template/template_1");
            fis = new FileInputStream(new File("src/test/resources/template.json"));
            newTemplate.setEntity(new StringEntity(Joiner.on("").join(IOUtils.readLines(fis)), Charset.forName("UTF-8")));
            executeRequest(newTemplate);
            HttpPut newMapping = new HttpPut("janusgraph");
            executeRequest(newMapping);
            final Duration maxWrite = Duration.ofMillis(2000L);
            final String storeName = "test_mapping";
            CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
            ModifiableConfiguration config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, cc,
                    BasicConfiguration.Restriction.NONE);
            config.set(USE_EXTERNAL_MAPPINGS, true, INDEX_NAME);
            Configuration indexConfig = config.restrictTo(INDEX_NAME);
            IndexProvider idx = new ElasticSearchIndex(indexConfig);
            final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest
                    .getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD));
            BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
            IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
            // Test date property OK
            idx.register(storeName, "date", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("date"), itx);
            // Test weight property KO
            try {
                idx.register(storeName, "weight", IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_ENGLISH, ANALYZER_KEYWORD).get("weight"), itx);
                fail("should failed");
            } catch (BackendException e) {
            }
        } catch (IOException e) {
            fail(e.getMessage());
        } finally {
            IOUtils.closeQuietly(fis);
            try {
                executeRequest(new HttpDelete("_template/template_1"));
            } catch (IOException e) {
                fail(e.getMessage());
            }
        }
    }

    private void simpleWriteAndQuery(IndexProvider idx) throws BackendException, InterruptedException {

        final Duration maxWrite = Duration.ofMillis(2000L);
        final String storeName = "jvmlocal_test_store";
        final KeyInformation.IndexRetriever indexRetriever = IndexProviderTest.getIndexRetriever(IndexProviderTest.getMapping(idx.getFeatures(), ANALYZER_STANDARD, ANALYZER_KEYWORD));

        BaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        IndexTransaction itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        assertEquals(0, itx.query(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "ali"))).size());
        itx.add(storeName, "doc", IndexProviderTest.NAME, "alice", false);
        itx.commit();
        Thread.sleep(1500L); // Slightly longer than default 1s index.refresh_interval
        itx = new IndexTransaction(idx, indexRetriever, txConfig, maxWrite);
        assertEquals(0, itx.query(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "zed"))).size());
        assertEquals(1, itx.query(new IndexQuery(storeName, PredicateCondition.of(IndexProviderTest.NAME, Text.PREFIX, "ali"))).size());
        itx.rollback();
    }

    private void executeRequest(HttpRequestBase request) throws IOException {
        CloseableHttpResponse res = null;
        try {
            res = httpClient.execute(host, request);
            assertTrue(res.getStatusLine().getStatusCode() >= 200);
            assertTrue(res.getStatusLine().getStatusCode() < 300);
            assertFalse(EntityUtils.toString(res.getEntity()).contains("error"));
        } finally {
            IOUtils.closeQuietly(res);
        }
    }

    private IndexProvider open(Configuration indexConfig) throws BackendException {
        final ElasticSearchIndex idx = new ElasticSearchIndex(indexConfig);
        idx.clearStorage();
        idx.close();
        return new ElasticSearchIndex(indexConfig);
    }

}
