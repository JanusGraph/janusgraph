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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.USE_DEPRECATED_MULTITYPE_INDEX;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public class ElasticSearchMultiTypeIndexTest extends ElasticSearchIndexTest {

    @Before @Override
    public void setUp() throws Exception {
        clear();
        super.setUp();
    }

    @After @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clear();
    }

    private void clear() throws Exception {
        try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpHost host = new HttpHost(InetAddress.getByName(esr.getHostname()), ElasticsearchRunner.PORT);
            IOUtils.closeQuietly(httpClient.execute(host, new HttpDelete("janusgraph*")));
        }
    }

    public Configuration getESTestConfig() {
        final String index = "es";
        final CommonsConfiguration cc = new CommonsConfiguration(new BaseConfiguration());
        if (esr.getEsMajorVersion().value > 2) {
            cc.set("index." + index + ".elasticsearch.ingest-pipeline.ingestvertex", "pipeline_1");
        }
        final ModifiableConfiguration config = esr.setElasticsearchConfiguration(new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,cc, BasicConfiguration.Restriction.NONE), index);
        config.set(USE_DEPRECATED_MULTITYPE_INDEX, true, index);
        config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index);
        return config.restrictTo(index);
    }

}
