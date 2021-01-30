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

import io.github.artsok.RepeatedIfExceptionsTest;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SolrJanusGraphIndexTest extends JanusGraphIndexTest {

    @Container
    protected static JanusGraphSolrContainer solrContainer = new JanusGraphSolrContainer();

    protected SolrJanusGraphIndexTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return solrContainer.getLocalSolrTestConfig(getStorageConfiguration(), getIndexBackends()).getConfiguration();
    }

    public abstract ModifiableConfiguration getStorageConfiguration();

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @Override
    public boolean supportsGeoPointExistsQuery() {
        return false;
    }

    @Override
    public String getStringField(String propertyKey) {
        return propertyKey + "_s";
    }

    @Override
    public String getTextField(String propertyKey) {
        return propertyKey + "_t";
    }

    @Test
    public void testRawQueries() {
        clopen(option(SolrIndex.DYNAMIC_FIELDS,JanusGraphIndexTest.INDEX),false);
        super.testRawQueries();
    }

    /*
     * Dropping collection is not implemented with Solr Cloud to accommodate use case where collection is created
     * outside of JanusGraph and associated with a config set with a different name.
     */
    @Override @Test @Disabled
    public void testClearStorage() throws Exception {
        super.testClearStorage();
    }

    @Override
    @RepeatedIfExceptionsTest(repeats = 10, suspend = 1000L)
    public void testIndexReplay() throws Exception {
        super.testIndexReplay();
    }

}
