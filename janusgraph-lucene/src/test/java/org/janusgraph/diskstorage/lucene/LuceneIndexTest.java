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

package org.janusgraph.diskstorage.lucene;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.time.Duration;
import java.util.HashMap;

import java.util.Map;

import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.KeyInformation.IndexRetriever;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LuceneIndexTest extends IndexProviderTest {

    private static final Logger log =
            LoggerFactory.getLogger(LuceneIndexTest.class);

    private static char REPLACEMENT_CHAR = '\u2022';
    private static final String MAPPING = "mapping";

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new LuceneIndex(getLocalLuceneTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public String getEnglishAnalyzerName() {
        return org.apache.lucene.analysis.en.EnglishAnalyzer.class.getName();
    }
    
    @Override
    public String getKeywordAnalyzerName() {
        return org.apache.lucene.analysis.core.KeywordAnalyzer.class.getName();
    }

    @Override
    public Mapping preferredGeoShapeMapping() {
        return Mapping.PREFIX_TREE;
    }

    public static Configuration getLocalLuceneTestConfig() {
        final String index = "lucene";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"),index);
        return config.restrictTo(index);
    }

    @Test
    public void testSupport() {
        // DEFAULT(=TEXT) support
        assertTrue( index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue( index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS_PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS_REGEX)); // TODO Not supported yet
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        // Same tests as above, except explicitly specifying TEXT instead of relying on DEFAULT
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS_FUZZY));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.CONTAINS_REGEX)); // TODO Not supported yet
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Text.PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Cmp.EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.TEXT)), Cmp.NOT_EQUAL));

        // STRING support
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.CONTAINS));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Text.FUZZY));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.STRING)), Cmp.NOT_EQUAL));

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
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE), Geo.DISJOINT));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.WITHIN));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.CONTAINS));
        assertTrue(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.INTERSECT));
        assertFalse(index.supports(of(Geoshape.class, Cardinality.SINGLE, new Parameter<>(MAPPING, Mapping.PREFIX_TREE)), Geo.DISJOINT));
    }


    @Test
    public void testMapKey2Field_IllegalCharacter() {
        assertThrows(IllegalArgumentException.class, () ->{
            index.mapKey2Field("here is an illegal character: " + REPLACEMENT_CHAR, null);
        });
    }

    @Test
    public void testMapKey2Field_MappingSpaces() {
        String expected = "field" + REPLACEMENT_CHAR + "name" + REPLACEMENT_CHAR + "with" + REPLACEMENT_CHAR + "spaces";
        assertEquals(expected, index.mapKey2Field("field name with spaces", null));
    }

    @Test
    public void testUsesKeywordAnalyzerAfterAddingIndexKey() throws Exception {
        final String newKeyword = "new_keyword";
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> initialDoc = HashMultimap.create();
        initialDoc.put(STRING, "Tom and Jerry");
        add(store, "doc1", initialDoc, true);
        clopen();

        // First run a query so that the KeyInformation gets used.
        // It will be cached in the LuceneCustomAnalyzer.
        IndexQuery query = new IndexQuery(store, PredicateCondition.of(STRING, Cmp.EQUAL, "Tom and Jerry"));
        assertEquals(1, tx.queryStream(query).count(), query.toString());

        // Add newKeyword to KeyInformation and create a new retriever
        final BaseTransactionConfig config = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        Map<String, KeyInformation> newKeys = new HashMap<>(allKeys);
        newKeys.put(newKeyword, new StandardKeyInformation(String.class, Cardinality.SINGLE,
            Mapping.TEXT.asParameter(), new Parameter<>(ParameterType.TEXT_ANALYZER.getName(),
            getKeywordAnalyzerName())));
        IndexRetriever indexRetriever = getIndexRetriever(newKeys);

        // Create doc using newKeyword
        IndexTransaction newTx = new IndexTransaction(index, indexRetriever, config, Duration.ofMillis(2000L));
        final IndexEntry idx = new IndexEntry(newKeyword, "Tom and Jerry");
        newTx.add(store, "doc2", idx, true);
        newTx.commit();

        newTx = new IndexTransaction(index, indexRetriever, config, Duration.ofMillis(2000L));

        // Verify the doc has been added
        query = new IndexQuery(store, PredicateCondition.of(newKeyword, Text.CONTAINS, "Tom and Jerry"));
        assertEquals(1, newTx.queryStream(query).count(), query.toString());

        // All the following queries should return zero results if the KeywordAnalyzer is being used
        query = new IndexQuery(store, PredicateCondition.of(newKeyword, Text.CONTAINS, "Tom Jerry"));
        assertEquals(0, newTx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(newKeyword, Text.CONTAINS, "Tom"));
        assertEquals(0, newTx.queryStream(query).count(), query.toString());
        query = new IndexQuery(store, PredicateCondition.of(newKeyword, Text.CONTAINS_PREFIX, "jerr"));
        assertEquals(0, newTx.queryStream(query).count(), query.toString());
    }
}
