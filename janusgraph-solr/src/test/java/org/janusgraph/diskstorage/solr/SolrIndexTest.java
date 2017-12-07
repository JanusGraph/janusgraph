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

import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrIndexTest extends IndexProviderTest {

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        SolrRunner.start();
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
    }

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new SolrIndex(getLocalSolrTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public String getEnglishAnalyzerName() {
        return WhitespaceTokenizer.class.getName();
    }

    @Override
    public String getKeywordAnalyzerName() {
        return KeywordTokenizer.class.getName();
    }

    private Configuration getLocalSolrTestConfig() {
        final String index = "solr";
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();

        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getZookeeperUrls(), index);
        config.set(SolrIndex.WAIT_SEARCHER, true, index);
        config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index);
        return config.restrictTo(index);
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE)));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING))));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXTSTRING))));

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
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXTSTRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXT)), Text.CONTAINS));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.DEFAULT)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.STRING)), Cmp.NOT_EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping",Mapping.TEXTSTRING)), Cmp.NOT_EQUAL));

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

    /**
     * Dropping collection is not implemented with Solr Cloud to accommodate use case where collection is created
     * outside of JanusGraph and associated with a config set with a different name.
     * @throws Exception
     */
    @Override @Test @Ignore
    public void clearStorageTest() throws Exception { }


    @Test(expected = IllegalArgumentException.class)
    public void testMapKey2Field_IllegalCharacter() {
        KeyInformation keyInfo = new StandardKeyInformation(Boolean.class, Cardinality.SINGLE);
        index.mapKey2Field("here is an illegal character: •", keyInfo);
    }

    @Test
    public void testMapKey2Field_MappingSpaces() {
        KeyInformation keyInfo = new StandardKeyInformation(Boolean.class, Cardinality.SINGLE);
        assertEquals("field•name•with•spaces_b", index.mapKey2Field("field name with spaces", keyInfo));
    }
}
