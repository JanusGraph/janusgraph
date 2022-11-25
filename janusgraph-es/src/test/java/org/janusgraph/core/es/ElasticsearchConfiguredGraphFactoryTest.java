// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.core.es;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.AbstractConfiguredGraphFactoryTest;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.converter.ReadConfigurationConverter;
import org.janusgraph.diskstorage.es.JanusGraphElasticsearchContainer;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.IOUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Testcontainers
public class ElasticsearchConfiguredGraphFactoryTest extends AbstractConfiguredGraphFactoryTest {

    private String indexBackendName = "search";
    @Container
    public static final JanusGraphElasticsearchContainer esContainer = new JanusGraphElasticsearchContainer();

    protected MapConfiguration getManagementConfig() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "inmemory");
        WriteConfiguration writeConf = esContainer.setConfiguration(config, indexBackendName).getConfiguration();
        return ReadConfigurationConverter.getInstance().convertToMapConfiguration(writeConf);
    }

    protected MapConfiguration getTemplateConfig() {
        return getManagementConfig();
    }

    protected MapConfiguration getGraphConfig() {
        final Map<String, Object> map = getTemplateConfig().getMap();
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        return ConfigurationUtil.loadMapConfiguration(map);
    }

    private boolean indexExists(String name) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpHost host = new HttpHost(InetAddress.getByName(esContainer.getHostname()), esContainer.getPort());
        final CloseableHttpResponse response = httpClient.execute(host, new HttpHead(name));
        final boolean exists = response.getStatusLine().getStatusCode() == 200;
        IOUtils.closeQuietly(response);
        return exists;
    }

    @Test
    public void indexNameShouldContainGraphName() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);

            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            assertNotNull(graph);

            String graphIndexName = "verticesByName";
            JanusGraphManagement mgmt = graph.openManagement();
            PropertyKey key = mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.buildIndex(graphIndexName, Vertex.class).addKey(key).buildMixedIndex(indexBackendName);
            mgmt.commit();

            String expectedIndexName = graphName + "_" + graphIndexName.toLowerCase();
            assertTrue(indexExists(expectedIndexName));
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }
}
