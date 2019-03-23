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

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Arrays;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.USE_DEPRECATED_MULTITYPE_INDEX;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
@EnabledIf("org.janusgraph.diskstorage.es.JanusGraphElasticsearchContainer.getEsMajorVersion().value < 6")
public class ElasticsearchMultiTypeIndexTest extends ElasticsearchIndexTest {

    @BeforeEach @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clear();
    }

    private void clear() throws Exception {
        try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpHost host = new HttpHost(InetAddress.getByName(esr.getHostname()), esr.getPort());
            IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("janusgraph*")));
        }
    }

    public Configuration getESTestConfig() {
        final String index = "es";
        final CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        cc.set("index." + index + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        final ModifiableConfiguration config = esr.setConfiguration(new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,cc, BasicConfiguration.Restriction.NONE), index);
        config.set(USE_DEPRECATED_MULTITYPE_INDEX, true, index);
        config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index);
        return config.restrictTo(index);
    }

    @Test @Override
    public void clearStorageTest() throws Exception {
        final String store = "vertex";
        initialize(store);
        final Multimap<String, Object> doc1 = getDocument("Hello world", 1001, 5.2, Geoshape.point(48.0, 0.0), Geoshape.polygon(Arrays.asList(new double[][] {{-0.1,47.9},{0.1,47.9},{0.1,48.1},{-0.1,48.1},{-0.1,47.9}})),Arrays.asList("1", "2", "3"), Sets.newHashSet("1", "2"), Instant.ofEpochSecond(1));
        add(store, "doc1", doc1, true);
        clopen();
        assertTrue(index.exists());
        super.tearDown();
        super.setUp();
        assertFalse(index.exists());
    }

}
