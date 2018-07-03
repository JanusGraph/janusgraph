package org.janusgraph.diskstorage.es;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.BULK_REFRESH;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.INTERFACE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_PORT;

public class ElasticsearchContainer extends GenericContainer {

    public static final Integer ELASTIC_PORT = 9200;
    public static final String DEFAULT_ELASTIC_VERSION = "6.0.1";
    public static final String DEFAULT_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

    private ElasticMajorVersion majorVersion;

    public ElasticMajorVersion getEsMajorVersion() {
        return majorVersion;
    }

    private static String getVersion() {
        String property = System.getProperty("elasticsearch.test.version");
        if (property != null)
            return property;
        return DEFAULT_ELASTIC_VERSION;
    }

    private static String getElasticImage() {
        String property = System.getProperty("es.docker.image");
        if (property != null)
            return property;
        return DEFAULT_IMAGE;
    }

    public ElasticsearchContainer() {
        super(getElasticImage() + ":" + getVersion());
    }

    @Override
    protected void configure() {
        addExposedPort(ELASTIC_PORT);
        addEnv("transport.host", "0.0.0.0");
        addEnv("discovery.type", "single-node");
        addEnv("xpack.security.enabled", "false");
        addEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        String groovy = System.getProperty("elasticsearch.test.groovy.inline");
        if (groovy != null && !groovy.equals("")) {
            addEnv("script.engine.groovy.inline.update", "true");
        }
        waitingFor(Wait.forHttp("/_cluster/health?timeout=30s&wait_for_status=yellow"));
        majorVersion = ElasticMajorVersion.parse(getVersion());
    }

    public String getHostname() {
        return getContainerIpAddress();
    }

    public Integer getPort() {
        return getMappedPort(ELASTIC_PORT);
    }


    ModifiableConfiguration setConfiguration(ModifiableConfiguration config, String index) {
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), index);
        config.set(INDEX_HOSTS, new String[]{getHostname()}, index);
        config.set(INDEX_PORT, getPort(), index);
        config.set(BULK_REFRESH, "wait_for", index);
        return config;
    }
}
