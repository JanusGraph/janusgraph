// Copyright 2020 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.FORCE_INDEX_USAGE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class ElasticsearchJanusGraphIndexTest extends JanusGraphIndexTest {

    @Container
    protected static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();

    public ElasticsearchJanusGraphIndexTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        String[] indexBackends = getIndexBackends();
        ModifiableConfiguration config =  esr.setConfiguration(getStorageConfiguration(), indexBackends);
        for (String indexBackend : indexBackends) {
            config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, indexBackend);
        }
        return config.getConfiguration();
    }

    public abstract ModifiableConfiguration getStorageConfiguration();

    @Test
    public void indexShouldExistAfterCreation() throws Exception {
        PropertyKey key = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("verticesByName", Vertex.class).addKey(key).buildMixedIndex("search");
        mgmt.commit();

        String expectedIndexName = INDEX_NAME.getDefaultValue() + "_" + "verticesByName".toLowerCase();
        assertTrue(esr.indexExists(expectedIndexName));
    }

    @Test
    public void indexShouldNotExistAfterDeletion() throws Exception {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ZERO,
            option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
            option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250),
            option(FORCE_INDEX_USAGE), true
        );

        String indexName = "mixed";
        String propertyName = "prop";

        makeKey(propertyName, String.class);
        finishSchema();

        //Never create new indexes while a transaction is active
        graph.getOpenTransactions().forEach(JanusGraphTransaction::rollback);
        mgmt = graph.openManagement();

        registerIndex(indexName, Vertex.class, propertyName);
        enableIndex(indexName);
        disableIndex(indexName);
        discardIndex(indexName);
        dropIndex(indexName);

        String expectedIndexName = INDEX_NAME.getName() + "_" + indexName.toLowerCase();
        assertFalse(esr.indexExists(expectedIndexName));
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @Override
    public boolean supportsGeoPointExistsQuery() {
        return true;
    }
}
