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
import com.google.common.collect.Multimap;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.attribute.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ElasticSearchIndexTest extends IndexProviderTest {

    static ElasticsearchRunner esr;
    static HttpHost host;
    static CloseableHttpClient httpClient;
    static ObjectMapper objectMapper;

    @BeforeClass
    public static void startElasticsearch() throws Exception {
        esr = new ElasticsearchRunner();
        esr.start();
        httpClient = HttpClients.createDefault();
        objectMapper = new ObjectMapper();
        host = new HttpHost(InetAddress.getByName(esr.getHostname()), ElasticsearchRunner.PORT);
        if (esr.getEsMajorVersion().value > 2) {
            IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("_ingest/pipeline/pipeline_1")));
            final HttpPut newPipeline = new HttpPut("_ingest/pipeline/pipeline_1");
            newPipeline.setHeader("Content-Type", "application/json");
            newPipeline.setEntity(new StringEntity("{\"description\":\"Test pipeline\",\"processors\":[{\"set\":{\"field\":\"" +STRING+ "\",\"value\":\"hello\"}}]}", Charset.forName("UTF-8")));
            IOUtils.closeQuietly(httpClient.execute(host, newPipeline));
        }
    }

    @AfterClass
    public static void stopElasticsearch() throws ClientProtocolException, IOException {
        IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("janusgraph*")));
        IOUtils.closeQuietly(httpClient);
        esr.stop();
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

    public Configuration getESTestConfig() {
        final String index = "es";
        final CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        if (esr.getEsMajorVersion().value > 2) {
            cc.set("index." + index + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        }
        return esr.setElasticsearchConfiguration(new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,cc, BasicConfiguration.Restriction.NONE), index)
            .set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index)
            .restrictTo(index);
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.CONTAINS_FUZZY));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, Mapping.TEXT.asParameter()), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, Mapping.STRING.asParameter()), Text.REGEX));
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
    }

    @Test
    public void testErrorInBatch() throws Exception {
        initialize("vertex");
        Multimap<String, Object> doc1 = HashMultimap.create();
        doc1.put(TIME, "not a time");

        add("vertex", "failing-doc", doc1, true);
        add("vertex", "non-failing-doc", getRandomDocument(), true);

        try {
            tx.commit();
            fail("Commit should not have succeeded.");
        } catch (JanusGraphException e) {
            // Looking for a NumberFormatException since we tried to stick a string of text into a time field.
            if (!Throwables.getRootCause(e).getMessage().contains("number_format_exception")
                && !Throwables.getRootCause(e).getMessage().contains("NumberFormatException")) {
                throw e;
            }
        } finally {
            tx = null;
        }
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
        if (esr.getEsMajorVersion().value > 2) {
            initialize("ingestvertex");
            final Multimap<String, Object> docs = HashMultimap.create();
            docs.put(TEXT, "bob");
            add("ingestvertex", "pipeline", docs, true);
            clopen();
            assertEquals(1, tx.queryStream(new IndexQuery("ingestvertex", PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).count());
            assertEquals(1, tx.queryStream(new IndexQuery("ingestvertex", PredicateCondition.of(STRING, Cmp.EQUAL, "hello"))).count());
        }
    }
}
