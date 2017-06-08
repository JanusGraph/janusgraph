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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.es.rest.RestElasticSearchClient;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CONF_FILE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_PORT;

/**
 * Create an ES {@link org.elasticsearch.client.transport.TransportClient} or
 * {@link org.elasticsearch.client.RestClient} from a JanusGraph
 * {@link org.janusgraph.diskstorage.configuration.Configuration}.
 * <p>
 * Assumes that an ES cluster is already running.  It does not attempt to start an
 * embedded ES instance.  It just connects to whatever hosts are given in
 * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
 * <p>
 * Setting arbitrary ES options is supported with the TransportClient
 * via {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_CONF_FILE}.
 * When this is set, it will be opened as an ordinary file and the contents will be
 * parsed as Elasticsearch settings.  These settings override JanusGraph's defaults but
 * options explicitly provided in JanusGraph's config file in JanusGraph's properties will
 * override any value that might be in the ES settings file.
 * <p>
 * After loading the index conf file (when provided), any key-value pairs under the
 * {@link org.janusgraph.diskstorage.es.ElasticSearchIndex#ES_EXTRAS_NS} namespace
 * are copied into the Elasticsearch settings builder.  This allows overridding arbitrary
 * ES settings from within the JanusGraph properties file.  Settings in the ext namespace take
 * precedence over those in the index conf file.
 * <p>
 * After loading the index conf file and any key-value pairs under the ext namespace,
 * JanusGraph checks for ConfigOptions defined in
 * {@link org.janusgraph.diskstorage.es.ElasticSearchIndex}
 * that correspond directly to ES settings and copies them into the ES settings builder.
 */
public enum ElasticSearchSetup {

    /**
     * Create an ES RestClient connected to
     * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
     */
    REST_CLIENT {
        @Override
        public Connection connect(Configuration config) throws IOException {
            log.debug("Configuring RestClient");

            final List<HttpHost> hosts = new ArrayList<>();
            int defaultPort = config.has(INDEX_PORT) ? config.get(INDEX_PORT) : ElasticSearchIndex.HOST_PORT_DEFAULT;
            for (String host : config.get(INDEX_HOSTS)) {
                String[] hostparts = host.split(":");
                String hostname = hostparts[0];
                int hostport = defaultPort;
                if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
                log.debug("Configured remote host: {} : {}", hostname, hostport);
                hosts.add(new HttpHost(hostname, hostport, "http"));
            }
            RestClient rc = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()])).build();

            RestElasticSearchClient client = new RestElasticSearchClient(rc);
            if (config.has(ElasticSearchIndex.BULK_REFRESH)) {
                client.setBulkRefresh(config.get(ElasticSearchIndex.BULK_REFRESH));
            }
            return new Connection(client);
        }
    };

    /**
     * Build and setup a new ES settings builder by consulting all JanusGraph config options
     * relevant to TransportClient or Node.  Options may be specific to a single client,
     * but in this case they have no effect/are ignored on the other client.
     * <p>
     * This method creates a new ES ImmutableSettings.Builder, then carries out the following
     * operations on that settings builder in the listed order:
     *
     * <ol>
     *  <li>Enable client.transport.ignore_cluster_name in the settings builder</li>
     *  <li>If conf-file is set, open it using a FileInputStream and load its contents into the settings builder</li>
     *  <li>Apply any settings in the ext.* meta namespace</li>
     *  <li>If cluster-name is set, copy that value to cluster.name in the settings builder</li>
     *  <li>If ignore-cluster-name is set, copy that value to client.transport.ignore_cluster_name in the settings builder</li>
     *  <li>If client-sniff is set, copy that value to client.transport.sniff in the settings builder</li>
     *  <li>If ttl-interval is set, copy that volue to indices.ttl.interval in the settings builder</li>
     *  <li>Unconditionally set script.inline to true (i.e. enable inline scripting)</li>
     * </ol>
     *
     * This method then returns the builder.
     *
     * @param config a JanusGraph configuration possibly containing Elasticsearch index settings
     * @return ES settings builder configured according to the {@code config} parameter
     * @throws java.io.IOException if conf-file was set but could not be read
     */
    private static Map<String,Object> settingsBuilder(Configuration config) throws IOException {

        Map<String,Object> settings = new HashMap<>();

        // Set JanusGraph defaults
        settings.put("client.transport.ignore_cluster_name", true);

        settings.put("path.home", System.getProperty("java.io.tmpdir"));

        // Apply overrides from ES conf file
        applySettingsFromFile(settings, config, INDEX_CONF_FILE);

        // Apply ext.* overrides from JanusGraph conf file
        applySettingsFromJanusGraphConf(settings, config, ElasticSearchIndex.ES_EXTRAS_NS);

        // Apply individual JanusGraph ConfigOptions that map to ES settings

        if (config.has(ElasticSearchIndex.CLUSTER_NAME)) {
            String clustername = config.get(ElasticSearchIndex.CLUSTER_NAME);
            Preconditions.checkArgument(StringUtils.isNotBlank(clustername), "Invalid cluster name: %s", clustername);
            String k = "cluster.name";
            settings.put(k, clustername);
            log.debug("Set {}: {}", k, clustername);
        }

        if (config.has(ElasticSearchIndex.IGNORE_CLUSTER_NAME)) {
            boolean ignoreClusterName = config.get(ElasticSearchIndex.IGNORE_CLUSTER_NAME);
            String k = "client.transport.ignore_cluster_name";
            settings.put(k, ignoreClusterName);
            log.debug("Set {}: {}", k, ignoreClusterName);
        }

        // Force-enable inline scripting.  This is probably only useful in Node mode.
        String inlineScriptsKey = "script.inline";
        String inlineScriptsVal = (String) settings.get(inlineScriptsKey);
        if (null != inlineScriptsVal && !"true".equals(inlineScriptsVal)) {
            log.error("JanusGraph requires Elasticsearch inline scripting but found {} set to false", inlineScriptsKey);
            throw new IOException("JanusGraph requires Elasticsearch inline scripting");
        }
        settings.put(inlineScriptsKey, true);
        log.debug("Set {}: {}", inlineScriptsKey, false);

        return settings;
    }

    static void applySettingsFromFile(Map<String,Object> settings,
                                              Configuration config,
                                              ConfigOption<String> confFileOption) throws IOException {
        if (config.has(confFileOption)) {
            String confFile = config.get(confFileOption);
            log.debug("Loading Elasticsearch settings from file {}", confFile);
            InputStream confStream = null;
            try {
                confStream = new FileInputStream(confFile);
                final Properties properties = new Properties();
                properties.load(confStream);
                properties.stringPropertyNames().stream().forEach(key -> settings.put(key, properties.get(key)));
            } finally {
                IOUtils.closeQuietly(confStream);
            }
        } else {
            log.debug("Option {} is not set; not attempting to load Elasticsearch settings from file", confFileOption);
        }
    }

    static void applySettingsFromJanusGraphConf(Map<String,Object> settings,
                                                     Configuration config,
                                                     ConfigNamespace rootNS) {
        int keysLoaded = 0;
        Map<String,Object> configSub = config.getSubset(rootNS);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (null == val) continue;
            if (List.class.isAssignableFrom(val.getClass())) {
                // Pretty print lists using comma-separated values and no surrounding square braces for ES
                List l = (List) val;
                settings.put(key, Joiner.on(",").join(l));
            } else if (val.getClass().isArray()) {
                // As with Lists, but now for arrays
                // The Object copy[] business lets us avoid repetitive primitive array type checking and casting
                Object copy[] = new Object[Array.getLength(val)];
                for (int i= 0; i < copy.length; i++) {
                    copy[i] = Array.get(val, i);
                }
                settings.put(key, Joiner.on(",").join(copy));
            } else {
                // Copy anything else unmodified
                settings.put(key, val.toString());
            }
            log.debug("[ES ext.* cfg] Set {}: {}", key, val);
            keysLoaded++;
        }
        log.debug("Loaded {} settings from the {} JanusGraph config namespace", keysLoaded, rootNS);
    }

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchSetup.class);

    public abstract Connection connect(Configuration config) throws IOException;

    public static class Connection {

        private final ElasticSearchClient client;

        public Connection(ElasticSearchClient client) {
            this.client = client;
            Preconditions.checkNotNull(this.client, "Unable to instantiate Elasticsearch Client object");
        }

        public ElasticSearchClient getClient() {
            return client;
        }
    }
}
