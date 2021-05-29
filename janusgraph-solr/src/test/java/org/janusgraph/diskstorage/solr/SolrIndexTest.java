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
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexProviderTest;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.UUID;

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
        return WhitespaceTokenizer.class.getName();
    }

    @Override
    public String getKeywordAnalyzerName() {
        return KeywordTokenizer.class.getName();
    }

    protected abstract Configuration getSolrTestConfig();

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
}
