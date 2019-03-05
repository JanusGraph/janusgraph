// Copyright 2019 JanusGraph Authors
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

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.BULK_REFRESH;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.INTERFACE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_PORT;

public class JanusGraphElasticsearchContainer extends ElasticsearchContainer {

    private static final Integer ELASTIC_PORT = 9200;
    private static final String DEFAULT_VERSION = "6.6.0";
    private static final String DEFAULT_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

    public static ElasticMajorVersion getEsMajorVersion() {
        return ElasticMajorVersion.parse(getVersion());
    }

    private static String getVersion() {
        String property = System.getProperty("elasticsearch.docker.version");
        if (property != null)
            return property;
        return DEFAULT_VERSION;
    }

    private static String getElasticImage() {
        String property = System.getProperty("elasticsearch.docker.image");
        if (property != null)
            return property;
        return DEFAULT_IMAGE;
    }

    public JanusGraphElasticsearchContainer() {
        this(false);
    }

    public JanusGraphElasticsearchContainer(boolean bindToDefaultPort) {
        super(getElasticImage() + ":" + getVersion());
        withEnv("transport.host", "0.0.0.0");
        withEnv("xpack.security.enabled", "false");
        withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        if (getEsMajorVersion().value == 5) {
            withEnv("script.max_compilations_per_minute", "30");
        }
        if (getEsMajorVersion().value < 5) {
            withClasspathResourceMapping("elasticsearch-old.yml", "/usr/share/elasticsearch/config/elasticsearch.yml", BindMode.READ_ONLY);
        }

        if(bindToDefaultPort){
            addFixedExposedPort(ELASTIC_PORT, ELASTIC_PORT);
        }
    }

    public String getHostname() {
        return getContainerIpAddress();
    }

    public Integer getPort() {
        return getMappedPort(ELASTIC_PORT);
    }


    public ModifiableConfiguration setConfiguration(ModifiableConfiguration config, String index) {
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), index);
        config.set(INDEX_HOSTS, new String[]{getHostname()}, index);
        config.set(INDEX_PORT, getPort(), index);
        config.set(BULK_REFRESH, "wait_for", index);
        return config;
    }
}
