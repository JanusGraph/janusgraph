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

import org.apache.commons.lang.RandomStringUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.attribute.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.BULK_REFRESH;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.INTERFACE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ElasticSearchIndexTest extends IndexProviderTest {

    private static ElasticsearchRunner esr;

    @BeforeClass
    public static void startElasticsearch() {
        if (!ElasticsearchRunner.IS_EXTERNAL) {
            esr = new ElasticsearchRunner();
            esr.start();
        }
    }

    @AfterClass
    public static void stopElasticsearch() {
        if (!ElasticsearchRunner.IS_EXTERNAL) {
            esr.stop();
        }
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
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), index);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1" }, index);
        config.set(BULK_REFRESH, "wait_for", index);
        config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index);
        return config.restrictTo(index);
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_FUZZY));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Cmp.NOT_EQUAL));

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
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.INTERSECT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.PREFIX_TREE)), Geo.DISJOINT));
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

        assertEquals("unescaped", tx.query(new IndexQuery("vertex", PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "$123"))).get(0));
        assertEquals("unescaped", tx.query(new IndexQuery("vertex", PredicateCondition.of(PHONE_SET, Cmp.EQUAL, "12345"))).get(0));
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

        assertEquals(1, tx.query(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).size());
        assertEquals(0, tx.query(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "world"))).size());

        tx.add("vertex", "long", TEXT, RandomStringUtils.randomAlphanumeric(500000) + " world " + RandomStringUtils.randomAlphanumeric(500000), false);

        clopen();

        assertEquals(0, tx.query(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "bob"))).size());
        assertEquals(1, tx.query(new IndexQuery("vertex", PredicateCondition.of(TEXT, Text.CONTAINS, "world"))).size());
    }
}
