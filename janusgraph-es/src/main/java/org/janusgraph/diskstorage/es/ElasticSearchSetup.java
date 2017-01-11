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
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.util.system.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Create an ES {@link org.elasticsearch.client.transport.TransportClient} or
 * {@link org.elasticsearch.node.Node} from a JanusGraph
 * {@link org.janusgraph.diskstorage.configuration.Configuration}.
 * <p>
 * TransportClient assumes that an ES cluster is already running.  It does not attempt
 * to start an embedded ES instance.  It just connects to whatever hosts are given in
 * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
 * <p>
 * Node can be configured to either behave strictly as a client or as both a client
 * and ES data node.  The latter is essentially a fully-fledged ES cluster node embedded in JanusGraph.
 * Node can also be configured to use either network or JVM local transport.
 * In practice, JVM local transport is usually only useful for testing.  Most deployments
 * will use the network transport.
 * <p>
 * Setting arbitrary ES options is supported with both TransportClient and Node
 * via {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_CONF_FILE}.
 * When this is set, it will be opened as an ordinary file and the contents will be
 * parsed as Elasticsearch settings.  These settings override JanusGraph's defaults but
 * options explicitly provided in JanusGraph's config file (e.g. setting an explicit value for
 * {@link org.janusgraph.diskstorage.es.ElasticSearchIndex#CLIENT_ONLY} in
 * JanusGraph's properties will override any value that might be in the ES settings file).
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
     * Start an ES TransportClient connected to
     * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
     */
    TRANSPORT_CLIENT {
        @Override
        public Connection connect(Configuration config) throws IOException {
            log.debug("Configuring TransportClient");

            ImmutableSettings.Builder settingsBuilder = settingsBuilder(config);

            if (config.has(ElasticSearchIndex.CLIENT_SNIFF)) {
                String k = "client.transport.sniff";
                settingsBuilder.put(k, config.get(ElasticSearchIndex.CLIENT_SNIFF));
                log.debug("Set {}: {}", k, config.get(ElasticSearchIndex.CLIENT_SNIFF));
            }

            TransportClient tc = new TransportClient(settingsBuilder.build());
            int defaultPort = config.has(INDEX_PORT) ? config.get(INDEX_PORT) : ElasticSearchIndex.HOST_PORT_DEFAULT;
            for (String host : config.get(INDEX_HOSTS)) {
                String[] hostparts = host.split(":");
                String hostname = hostparts[0];
                int hostport = defaultPort;
                if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
                log.info("Configured remote host: {} : {}", hostname, hostport);
                tc.addTransportAddress(new InetSocketTransportAddress(hostname, hostport));
            }
            return new Connection(null, tc);
        }
    },

    /**
     * Start an ES {@code Node} and use its attached {@code Client}.
     */
    NODE {
        @Override
        public Connection connect(Configuration config) throws IOException {

            log.debug("Configuring Node Client");

            ImmutableSettings.Builder settingsBuilder = settingsBuilder(config);

            if (config.has(ElasticSearchIndex.TTL_INTERVAL)) {
                String k = "indices.ttl.interval";
                settingsBuilder.put(k, config.get(ElasticSearchIndex.TTL_INTERVAL));
                log.debug("Set {}: {}", k, config.get(ElasticSearchIndex.TTL_INTERVAL));
            }

            makeLocalDirsIfNecessary(settingsBuilder, config);

            NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().settings(settingsBuilder.build());

            // Apply explicit JanusGraph properties file overrides (otherwise conf-file or ES defaults apply)
            if (config.has(ElasticSearchIndex.CLIENT_ONLY)) {
                boolean clientOnly = config.get(ElasticSearchIndex.CLIENT_ONLY);
                nodeBuilder.client(clientOnly).data(!clientOnly);
            }

            if (config.has(ElasticSearchIndex.LOCAL_MODE))
                nodeBuilder.local(config.get(ElasticSearchIndex.LOCAL_MODE));

            if (config.has(ElasticSearchIndex.LOAD_DEFAULT_NODE_SETTINGS))
                nodeBuilder.loadConfigSettings(config.get(ElasticSearchIndex.LOAD_DEFAULT_NODE_SETTINGS));

            Node node = nodeBuilder.node();
            Client client = node.client();
            return new Connection(node, client);
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
     *  <li>Unconditionally set script.disable_dynamic to false (i.e. enable dynamic scripting)</li>
     * </ol>
     *
     * This method then returns the builder.
     *
     * @param config a JanusGraph configuration possibly containing Elasticsearch index settings
     * @return ES settings builder configured according to the {@code config} parameter
     * @throws java.io.IOException if conf-file was set but could not be read
     */
    private static ImmutableSettings.Builder settingsBuilder(Configuration config) throws IOException {

        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();

        // Set JanusGraph defaults
        settings.put("client.transport.ignore_cluster_name", true);

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

        // Force-enable dynamic scripting.  This is probably only useful in Node mode.
        String disableScriptsKey = "script.disable_dynamic";
        String disableScriptsVal = settings.get(disableScriptsKey);
        if (null != disableScriptsVal && !"false".equals(disableScriptsVal)) {
            log.warn("JanusGraph requires Elasticsearch dynamic scripting.  Setting {} to false.  " +
                    "Dynamic scripting must be allowed in the Elasticsearch cluster configuration.",
                    disableScriptsKey);
        }
        settings.put(disableScriptsKey, false);
        log.debug("Set {}: {}", disableScriptsKey, false);

        return settings;
    }

    static void applySettingsFromFile(ImmutableSettings.Builder settings,
                                              Configuration config,
                                              ConfigOption<String> confFileOption) throws FileNotFoundException {
        if (config.has(confFileOption)) {
            String confFile = config.get(confFileOption);
            log.debug("Loading Elasticsearch settings from file {}", confFile);
            InputStream confStream = null;
            try {
                confStream = new FileInputStream(confFile);
                settings.loadFromStream(confFile, confStream);
            } finally {
                IOUtils.closeQuietly(confStream);
            }
        } else {
            log.debug("Option {} is not set; not attempting to load Elasticsearch settings from file", confFileOption);
        }
    }

    static void applySettingsFromJanusGraphConf(ImmutableSettings.Builder settings,
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


    private static void makeLocalDirsIfNecessary(ImmutableSettings.Builder settingsBuilder, Configuration config) {
        if (config.has(INDEX_DIRECTORY)) {
            String dataDirectory = config.get(INDEX_DIRECTORY);
            File f = new File(dataDirectory);
            if (!f.exists()) {
                log.info("Creating ES directory prefix: {}", f);
                f.mkdirs();
            }
            for (String sub : ElasticSearchIndex.DATA_SUBDIRS) {
                String subdir = dataDirectory + File.separator + sub;
                f = new File(subdir);
                if (!f.exists()) {
                    log.info("Creating ES {} directory: {}", sub, f);
                    f.mkdirs();
                }
                settingsBuilder.put("path." + sub, subdir);
                log.debug("Set ES {} directory: {}", sub, f);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchSetup.class);

    public abstract Connection connect(Configuration config) throws IOException;

    public static class Connection {

        private final Node node;
        private final Client client;

        public Connection(Node node, Client client) {
            this.node = node;
            this.client = client;
            Preconditions.checkNotNull(this.client, "Unable to instantiate Elasticsearch Client object");
            // node may be null
        }

        public Node getNode() {
            return node;
        }

        public Client getClient() {
            return client;
        }
    }
}
