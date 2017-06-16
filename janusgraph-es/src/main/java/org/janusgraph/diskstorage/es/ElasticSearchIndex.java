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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.attribute.*;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.util.system.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@PreInitializeConfigOptions
public class ElasticSearchIndex implements IndexProvider {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchIndex.class);

    private static final String TTL_FIELD = "_ttl";
    private static final String STRING_MAPPING_SUFFIX = "__STRING";

    public static final ImmutableList<String> DATA_SUBDIRS = ImmutableList.of("data", "work", "logs");

    public static final ConfigNamespace ELASTICSEARCH_NS =
            new ConfigNamespace(INDEX_NS, "elasticsearch", "Elasticsearch index configuration");

    public static final ConfigOption<Boolean> CLIENT_ONLY =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "client-only",
            "The Elasticsearch node.client option is set to this boolean value, and the Elasticsearch node.data " +
            "option is set to the negation of this value.  True creates a thin client which holds no data.  False " +
            "creates a regular Elasticsearch cluster node that may store data.",
            ConfigOption.Type.GLOBAL_OFFLINE, true);

    public static final ConfigOption<String> CLUSTER_NAME =
            new ConfigOption<String>(ELASTICSEARCH_NS, "cluster-name",
            "The name of the Elasticsearch cluster.  This should match the \"cluster.name\" setting " +
            "in the Elasticsearch nodes' configuration.", ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");

    public static final ConfigOption<Boolean> LOCAL_MODE =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "local-mode",
            "On the legacy config track, this option chooses between starting a TransportClient (false) or " +
            "a Node with JVM-local transport and local data (true).  On the interface config track, this option " +
            "is considered by (but optional for) the Node client and ignored by the TransportClient.  See the manual " +
            "for more information about ES config tracks.",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigOption<Boolean> CLIENT_SNIFF =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "sniff",
            "Whether to enable cluster sniffing.  This option only applies to the TransportClient.  " +
            "Enabling this option makes the TransportClient attempt to discover other cluster nodes " +
            "besides those in the initial host list provided at startup.", ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String> INTERFACE =
            new ConfigOption<>(ELASTICSEARCH_NS, "interface",
            "Whether to connect to ES using the Node or Transport client (see the \"Talking to Elasticsearch\" " +
            "section of the ES manual for discussion of the difference).  Setting this option enables the " +
            "interface config track (see manual for more information about ES config tracks).",
            ConfigOption.Type.MASKABLE, String.class, ElasticSearchSetup.TRANSPORT_CLIENT.toString(),
            disallowEmpty(String.class));

    public static final ConfigOption<Boolean> IGNORE_CLUSTER_NAME =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "ignore-cluster-name",
            "Whether to bypass validation of the cluster name of connected nodes.  " +
            "This option is only used on the interface configuration track (see manual for " +
            "information about ES config tracks).", ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String> TTL_INTERVAL =
            new ConfigOption<String>(ELASTICSEARCH_NS, "ttl-interval",
            "The period of time between runs of ES's bulit-in expired document deleter.  " +
            "This string will become the value of ES's indices.ttl.interval setting and should " +
            "be formatted accordingly, e.g. 5s or 60s.", ConfigOption.Type.MASKABLE, "5s");

    public static final ConfigOption<String> HEALTH_REQUEST_TIMEOUT =
            new ConfigOption<String>(ELASTICSEARCH_NS, "health-request-timeout",
            "When JanusGraph initializes its ES backend, JanusGraph waits up to this duration for the " +
            "ES cluster health to reach at least yellow status.  " +
            "This string should be formatted as a natural number followed by the lowercase letter " +
            "\"s\", e.g. 3s or 60s.", ConfigOption.Type.MASKABLE, "30s");

    public static final ConfigOption<Boolean> LOAD_DEFAULT_NODE_SETTINGS =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "load-default-node-settings",
            "Whether ES's Node client will internally attempt to load default configuration settings " +
            "from system properties/process environment variables.  Only meaningful when using the Node " +
            "client (has no effect with TransportClient).", ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> USE_EDEPRECATED_IGNORE_UNMAPPED_OPTION =
            new ConfigOption<>(ELASTICSEARCH_NS, "use-deprecated-ignore-unmapped-option",
            "Elasticsearch versions before 1.4.0 supported the \"ignore_unmapped\" sort option. " +
            "In 1.4.0, it was deprecated by the new \"unmapped_type\" sort option.  This configuration" +
            "setting controls which ES option JanusGraph uses: false for the newer \"unmapped_type\"," +
            "true for the older \"ignore_unmapped\".", ConfigOption.Type.MASKABLE, false);

    public static final ConfigNamespace ES_EXTRAS_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "ext", "Overrides for arbitrary elasticsearch.yaml settings", true);

    public static final ConfigNamespace ES_CREATE_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "create", "Settings related to index creation");

    public static final ConfigOption<Long> CREATE_SLEEP =
            new ConfigOption<Long>(ES_CREATE_NS, "sleep",
            "How long to sleep, in milliseconds, between the successful completion of a (blocking) index " +
            "creation request and the first use of that index.  This only applies when creating an index in ES, " +
            "which typically only happens the first time JanusGraph is started on top of ES. If the index JanusGraph is " +
            "configured to use already exists, then this setting has no effect.", ConfigOption.Type.MASKABLE, 200L);

    public static final ConfigNamespace ES_CREATE_EXTRAS_NS =
            new ConfigNamespace(ES_CREATE_NS, "ext", "Overrides for arbitrary settings applied at index creation", true);

    private static final IndexFeatures ES_FEATURES = new IndexFeatures.Builder().supportsDocumentTTL()
            .setDefaultStringMapping(Mapping.TEXT).supportedStringMappings(Mapping.TEXT, Mapping.TEXTSTRING, Mapping.STRING).setWildcardField("_all").supportsCardinality(Cardinality.SINGLE).supportsCardinality(Cardinality.LIST).supportsCardinality(Cardinality.SET).supportsNanoseconds().build();

    public static final int HOST_PORT_DEFAULT = 9300;

    private final Node node;
    private final Client client;
    private final String indexName;
    private final int maxResultsSize;
    private final boolean useDeprecatedIgnoreUnmapped;

    public ElasticSearchIndex(Configuration config) {
        indexName = config.get(INDEX_NAME);
        useDeprecatedIgnoreUnmapped = config.get(USE_EDEPRECATED_IGNORE_UNMAPPED_OPTION);

        checkExpectedClientVersion();

        final ElasticSearchSetup.Connection c;
        if (!config.has(INTERFACE)) {
            c = legacyConfiguration(config);
        } else {
            c = interfaceConfiguration(config);
        }
        node = c.getNode();
        client = c.getClient();

        maxResultsSize = config.get(INDEX_MAX_RESULT_SET_SIZE);
        log.debug("Configured ES query result set max size to {}", maxResultsSize);

        client.admin().cluster().prepareHealth().setTimeout(config.get(HEALTH_REQUEST_TIMEOUT))
                .setWaitForYellowStatus().execute().actionGet();

        checkForOrCreateIndex(config);
    }

    /**
     * If ES already contains this instance's target index, then do nothing.
     * Otherwise, create the index, then wait {@link #CREATE_SLEEP}.
     * <p>
     * The {@code client} field must point to a live, connected client.
     * The {@code indexName} field must be non-null and point to the name
     * of the index to check for existence or create.
     *
     * @param config the config for this ElasticSearchIndex
     * @throws java.lang.IllegalArgumentException if the index could not be created
     */
    private void checkForOrCreateIndex(Configuration config) {
        Preconditions.checkState(null != client);

        //Create index if it does not already exist
        IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
        if (!response.isExists()) {

            ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();

            ElasticSearchSetup.applySettingsFromJanusGraphConf(settings, config, ES_CREATE_EXTRAS_NS);

            CreateIndexResponse create = client.admin().indices().prepareCreate(indexName)
                    .setSettings(settings.build()).execute().actionGet();
            try {
                final long sleep = config.get(CREATE_SLEEP);
                log.debug("Sleeping {} ms after {} index creation returned from actionGet()", sleep, indexName);
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                throw new JanusGraphException("Interrupted while waiting for index to settle in", e);
            }
            if (!create.isAcknowledged()) throw new IllegalArgumentException("Could not create index: " + indexName);
        }
    }


    /**
     * Configure ElasticSearchIndex's ES client according to semantics introduced in
     * 0.5.1.  Allows greater flexibility than the previous config semantics.  See
     * {@link org.janusgraph.diskstorage.es.ElasticSearchSetup} for more
     * information.
     * <p>
     * This is activated by setting an explicit value for {@link #INTERFACE} in
     * the JanusGraph configuration.
     *
     * @see #legacyConfiguration(org.janusgraph.diskstorage.configuration.Configuration)
     * @param config a config passed to ElasticSearchIndex's constructor
     * @return a node and client object open and ready for use
     */
    private ElasticSearchSetup.Connection interfaceConfiguration(Configuration config) {
        ElasticSearchSetup clientMode = ConfigOption.getEnumValue(config.get(INTERFACE), ElasticSearchSetup.class);

        try {
            return clientMode.connect(config);
        } catch (IOException e) {
            throw new JanusGraphException(e);
        }
    }

    /**
     * Configure ElasticSearchIndex's ES client according to 0.4.x - 0.5.0 semantics.
     * This checks local-mode first.  If local-mode is true, then it creates a Node that
     * uses JVM local transport and can't talk over the network.  If local-mode is
     * false, then it creates a TransportClient that can talk over the network and
     * uses {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}
     * as the server addresses.  Note that this configuration method
     * does not allow creating a Node that talks over the network.
     * <p>
     * This is activated by <b>not</b> setting an explicit value for {@link #INTERFACE} in the
     * JanusGraph configuration.
     *
     * @see #interfaceConfiguration(org.janusgraph.diskstorage.configuration.Configuration)
     * @param config a config passed to ElasticSearchIndex's constructor
     * @return a node and client object open and ready for use
     */
    private ElasticSearchSetup.Connection legacyConfiguration(Configuration config) {
        Node node;
        Client client;

        if (config.get(LOCAL_MODE)) {

            log.debug("Configuring ES for JVM local transport");

            boolean clientOnly = config.get(CLIENT_ONLY);
            boolean local = config.get(LOCAL_MODE);

            NodeBuilder builder = NodeBuilder.nodeBuilder();
            Preconditions.checkArgument(config.has(INDEX_CONF_FILE) || config.has(INDEX_DIRECTORY),
                    "Must either configure configuration file or base directory");
            if (config.has(INDEX_CONF_FILE)) {
                String configFile = config.get(INDEX_CONF_FILE);
                ImmutableSettings.Builder sb = ImmutableSettings.settingsBuilder();
                log.debug("Configuring ES from YML file [{}]", configFile);
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(configFile);
                    sb.loadFromStream(configFile, fis);
                    builder.settings(sb.build());
                } catch (FileNotFoundException e) {
                    throw new JanusGraphException(e);
                } finally {
                    IOUtils.closeQuietly(fis);
                }
            } else {
                String dataDirectory = config.get(INDEX_DIRECTORY);
                log.debug("Configuring ES with data directory [{}]", dataDirectory);
                File f = new File(dataDirectory);
                if (!f.exists()) f.mkdirs();
                ImmutableSettings.Builder b = ImmutableSettings.settingsBuilder();
                for (String sub : DATA_SUBDIRS) {
                    String subdir = dataDirectory + File.separator + sub;
                    f = new File(subdir);
                    if (!f.exists()) f.mkdirs();
                    b.put("path." + sub, subdir);
                }
                b.put("script.disable_dynamic", false);
                b.put("indices.ttl.interval", "5s");

                builder.settings(b.build());

                String clustername = config.get(CLUSTER_NAME);
                Preconditions.checkArgument(StringUtils.isNotBlank(clustername), "Invalid cluster name: %s", clustername);
                builder.clusterName(clustername);
            }

            node = builder.client(clientOnly).data(!clientOnly).local(local).node();
            client = node.client();

        } else {
            log.debug("Configuring ES for network transport");
            ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
            if (config.has(CLUSTER_NAME)) {
                String clustername = config.get(CLUSTER_NAME);
                Preconditions.checkArgument(StringUtils.isNotBlank(clustername), "Invalid cluster name: %s", clustername);
                settings.put("cluster.name", clustername);
            } else {
                settings.put("client.transport.ignore_cluster_name", true);
            }
            log.debug("Transport sniffing enabled: {}", config.get(CLIENT_SNIFF));
            settings.put("client.transport.sniff", config.get(CLIENT_SNIFF));
            settings.put("script.disable_dynamic", false);
            TransportClient tc = new TransportClient(settings.build());
            int defaultPort = config.has(INDEX_PORT)?config.get(INDEX_PORT):HOST_PORT_DEFAULT;
            for (String host : config.get(INDEX_HOSTS)) {
                String[] hostparts = host.split(":");
                String hostname = hostparts[0];
                int hostport = defaultPort;
                if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
                log.info("Configured remote host: {} : {}", hostname, hostport);
                tc.addTransportAddress(new InetSocketTransportAddress(hostname, hostport));
            }
            client = tc;
            node = null;
        }

        return new ElasticSearchSetup.Connection(node, client);
    }

    private BackendException convert(Exception esException) {
        if (esException instanceof InterruptedException) {
            return new TemporaryBackendException("Interrupted while waiting for response", esException);
        } else {
            return new PermanentBackendException("Unknown exception while executing index operation", esException);
        }
    }

    private static String getDualMappingName(String key) {
        return key + STRING_MAPPING_SUFFIX;
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        XContentBuilder mapping;
        Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map==Mapping.DEFAULT || AttributeUtil.isString(dataType),
                "Specified illegal mapping [%s] for data type [%s]",map,dataType);

        try {
            mapping = XContentFactory.jsonBuilder().
                    startObject().
                    startObject(store).
                    field(TTL_FIELD, new HashMap<String, Object>() {{
                        put("enabled", true);
                    }}).
                    startObject("properties").
                    startObject(key);

            if (AttributeUtil.isString(dataType)) {
                if (map==Mapping.DEFAULT) map=Mapping.TEXT;
                log.debug("Registering string type for {} with mapping {}", key, map);
                mapping.field("type", "string");
                switch (map) {
                    case STRING:
                        mapping.field("index","not_analyzed");
                        break;
                    case TEXT:
                        //default, do nothing
                    	break;
                    case TEXTSTRING:
                        mapping.endObject();
                        //add string mapping
                        mapping.startObject(getDualMappingName(key));
                        mapping.field("type", "string");
                        mapping.field("index","not_analyzed");
                        break;
                    default: throw new AssertionError("Unexpected mapping: "+map);
                }
            } else if (dataType == Float.class) {
                log.debug("Registering float type for {}", key);
                mapping.field("type", "float");
            } else if (dataType == Double.class) {
                log.debug("Registering double type for {}", key);
                mapping.field("type", "double");
            } else if (dataType == Byte.class) {
                log.debug("Registering byte type for {}", key);
                mapping.field("type", "byte");
            } else if (dataType == Short.class) {
                log.debug("Registering short type for {}", key);
                mapping.field("type", "short");
            } else if (dataType == Integer.class) {
                log.debug("Registering integer type for {}", key);
                mapping.field("type", "integer");
            } else if (dataType == Long.class) {
                log.debug("Registering long type for {}", key);
                mapping.field("type", "long");
            } else if (dataType == Boolean.class) {
                log.debug("Registering boolean type for {}", key);
                mapping.field("type", "boolean");
            } else if (dataType == Geoshape.class) {
                log.debug("Registering geo_point type for {}", key);
                mapping.field("type", "geo_point");
            } else if (dataType == Date.class || dataType == Instant.class) {
                log.debug("Registering date type for {}", key);
                mapping.field("type", "date");
            } else if (dataType == Boolean.class) {
                log.debug("Registering boolean type for {}", key);
                mapping.field("type", "boolean");
            } else if (dataType == UUID.class) {
                log.debug("Registering uuid type for {}", key);
                mapping.field("type", "string");
                mapping.field("index","not_analyzed");
            }

            mapping.endObject().endObject().endObject().endObject();

        } catch (IOException e) {
            throw new PermanentBackendException("Could not render json for put mapping request", e);
        }

        try {
            PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).
                    setIgnoreConflicts(false).setType(store).setSource(mapping).execute().actionGet();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private static Mapping getStringMapping(KeyInformation information) {
        assert AttributeUtil.isString(information.getDataType());
        Mapping map = Mapping.getMapping(information);
        if (map==Mapping.DEFAULT) map = Mapping.TEXT;
        return map;
    }

    private static boolean hasDualStringMapping(KeyInformation information) {
        return AttributeUtil.isString(information.getDataType()) && getStringMapping(information)==Mapping.TEXTSTRING;
    }

    public XContentBuilder getNewDocument(final List<IndexEntry> additions, KeyInformation.StoreRetriever informations, int ttl) throws BackendException {
        Preconditions.checkArgument(ttl >= 0);
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            // JSON writes duplicate fields one after another, which forces us
            // at this stage to make de-duplication on the IndexEntry list. We don't want to pay the
            // price map storage on the Mutation level because non of other backends need that.

            Multimap<String, IndexEntry> uniq = LinkedListMultimap.create();
            for (IndexEntry e : additions) {
                uniq.put(e.field, e);
            }

            for (Map.Entry<String, Collection<IndexEntry>> add : uniq.asMap().entrySet()) {
                KeyInformation keyInformation = informations.get(add.getKey());
                Object value = null;
                switch (keyInformation.getCardinality()) {
                    case SINGLE:
                        value = convertToEsType(Iterators.getLast(add.getValue().iterator()).value);
                        break;
                    case SET:
                    case LIST:
                        value = add.getValue().stream().map(v -> convertToEsType(v.value)).collect(Collectors.toList()).toArray();
                        break;
                }


                builder.field(add.getKey(), value);
                if (hasDualStringMapping(informations.get(add.getKey())) && keyInformation.getDataType() == String.class) {
                    builder.field(getDualMappingName(add.getKey()), value);
                }


            }
            if (ttl>0) builder.field(TTL_FIELD, TimeUnit.MILLISECONDS.convert(ttl,TimeUnit.SECONDS));

            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new PermanentBackendException("Could not write json");
        }
    }

    private static Object convertToEsType(Object value) {
        if (value instanceof Number) {
            if (AttributeUtil.isWholeNumber((Number) value)) {
                return ((Number) value).longValue();
            } else { //double or float
                return ((Number) value).doubleValue();
            }
        } else if (AttributeUtil.isString(value)) {
            return value;
        } else if (value instanceof Geoshape) {
            Geoshape shape = (Geoshape) value;
            if (shape.getType() == Geoshape.Type.POINT) {
                Geoshape.Point p = shape.getPoint();
                return new double[]{p.getLongitude(), p.getLatitude()};
            } else throw new UnsupportedOperationException("Geo type is not supported: " + shape.getType());

        } else if (value instanceof Date || value instanceof Instant) {
            return value;
        } else if (value instanceof Boolean) {
            return value;
        } else if (value instanceof UUID) {
            return value.toString();
        } else throw new IllegalArgumentException("Unsupported type: " + value.getClass() + " (value: " + value + ")");
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        BulkRequestBuilder brb = client.prepareBulk();

        int bulkrequests = 0;
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String storename = stores.getKey();
                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {

                    String docid = entry.getKey();
                    IndexMutation mutation = entry.getValue();
                    assert mutation.isConsolidated();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());
                    //Deletions first
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            log.trace("Deleting entire document {}", docid);
                            brb.add(new DeleteRequest(indexName, storename, docid));
                        } else {
                            String script = getDeletionScript(informations, storename, mutation);
                            brb.add(client.prepareUpdate(indexName, storename, docid).setScript(script, ScriptService.ScriptType.INLINE));
                            log.trace("Adding script {}", script);
                        }

                        bulkrequests++;
                    }
                    if (mutation.hasAdditions()) {
                        int ttl = mutation.determineTTL();

                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", docid);
                            brb.add(new IndexRequest(indexName, storename, docid)
                                    .source(getNewDocument(mutation.getAdditions(), informations.get(storename), ttl)));

                        } else {
                            Preconditions.checkArgument(ttl == 0, "Elasticsearch only supports TTL on new documents [%s]", docid);

                            boolean needUpsert = !mutation.hasDeletions();
                            String script = getAdditionScript(informations, storename, mutation);
                            UpdateRequestBuilder update = client.prepareUpdate(indexName, storename, docid).setScript(script, ScriptService.ScriptType.INLINE);
                            if (needUpsert) {
                                XContentBuilder doc = getNewDocument(mutation.getAdditions(), informations.get(storename), ttl);
                                update.setUpsert(doc);
                            }

                            brb.add(update);
                            log.trace("Adding script {}", script);
                        }

                        bulkrequests++;
                    }

                }
            }
            if (bulkrequests > 0) {
                BulkResponse bulkItemResponses = brb.execute().actionGet();
                if (bulkItemResponses.hasFailures()) {
                    boolean actualFailure = false;
                    for(BulkItemResponse response : bulkItemResponses.getItems()) {
                        //The document may have been deleted, which is OK
                        if(response.isFailed() && response.getFailure().getStatus() != RestStatus.NOT_FOUND) {
                            log.error("Failed to execute ES query {}", response.getFailureMessage());
                            actualFailure = true;
                        }
                    }
                    if(actualFailure) {
                        throw new Exception(bulkItemResponses.buildFailureMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to execute ES query {}", brb.request().timeout(), e);
            throw convert(e);
        }
    }

    private String getDeletionScript(KeyInformation.IndexRetriever informations, String storename, IndexMutation mutation) throws PermanentBackendException {
        StringBuilder script = new StringBuilder();
        for (IndexEntry deletion : mutation.getDeletions()) {
            KeyInformation keyInformation = informations.get(storename).get(deletion.field);

            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    script.append("ctx._source.remove(\"" + deletion.field + "\");");
                    if (hasDualStringMapping(informations.get(storename, deletion.field))) {
                        script.append("ctx._source.remove(\"" + getDualMappingName(deletion.field) + "\");");
                    }
                    break;
                case SET:
                case LIST:
                    String jsValue = convertToJsType(deletion.value);
                    script.append("def index = ctx._source[\"" + deletion.field + "\"].indexOf(" + jsValue + "); ctx._source[\"" + deletion.field + "\"].remove(index);");
                    if (hasDualStringMapping(informations.get(storename, deletion.field))) {
                        script.append("def index = ctx._source[\"" + getDualMappingName(deletion.field) + "\"].indexOf(" + jsValue + "); ctx._source[\"" + getDualMappingName(deletion.field) + "\"].remove(index);");
                    }
                    break;

            }
        }
        return script.toString();
    }

    private String getAdditionScript(KeyInformation.IndexRetriever informations, String storename, IndexMutation mutation) throws PermanentBackendException {
        StringBuilder script = new StringBuilder();
        for (IndexEntry e : mutation.getAdditions()) {
            KeyInformation keyInformation = informations.get(storename).get(e.field);
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    script.append("ctx._source[\"" + e.field + "\"] = " + convertToJsType(e.value) + ";");
                    if (hasDualStringMapping(keyInformation)) {
                        script.append("ctx._source[\"" + getDualMappingName(e.field) + "\"] = " + convertToJsType(e.value) + ";");
                    }
                    break;
                case SET:
                case LIST:
                    script.append("if(ctx._source[\"" + e.field + "\"] == null) {ctx._source[\"" + e.field + "\"] = []};");
                    script.append("ctx._source[\"" + e.field + "\"].add(" + convertToJsType(e.value) + ");");
                    if (hasDualStringMapping(keyInformation)) {
                        script.append("if(ctx._source[\"" + getDualMappingName(e.field) + "\"] == null) {ctx._source[\"" + e.field + "\"] = []};");
                        script.append("ctx._source[\"" + getDualMappingName(e.field) + "\"].add(" + convertToJsType(e.value) + ");");
                    }
                    break;

            }

        }
        return script.toString();
    }

    private static String convertToJsType(Object value) throws PermanentBackendException {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            builder.field("value", convertToEsType(value));

            String s = builder.string();
            int prefixLength = "{\"value\":".length();
            int suffixLength = "}".length();
            String result = s.substring(prefixLength, s.length() - suffixLength);
            result = result.replace("$", "\\$");
            return result;
        } catch (IOException e) {
            throw new PermanentBackendException("Could not write json");
        }


    }


    public void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        BulkRequestBuilder bulk = client.prepareBulk();
        int requests = 0;
        try {
            for (Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                String store = stores.getKey();

                for (Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    String docID = entry.getKey();
                    List<IndexEntry> content = entry.getValue();

                    if (content == null || content.size() == 0) {
                        // delete
                        if (log.isTraceEnabled())
                            log.trace("Deleting entire document {}", docID);

                        bulk.add(new DeleteRequest(indexName, store, docID));
                        requests++;
                    } else {
                        // Add
                        if (log.isTraceEnabled())
                            log.trace("Adding entire document {}", docID);
                        bulk.add(new IndexRequest(indexName, store, docID).source(getNewDocument(content, informations.get(store), IndexMutation.determineTTL(content))));
                        requests++;
                    }
                }
            }

            if (requests > 0)
                bulk.execute().actionGet();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public FilterBuilder getFilter(Condition<?> condition, KeyInformation.StoreRetriever informations) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<String, ?> atom = (PredicateCondition) condition;
            Object value = atom.getValue();
            String key = atom.getKey();
            JanusGraphPredicate janusgraphPredicate = atom.getPredicate();
            if (value instanceof Number) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on numeric types: " + janusgraphPredicate);
                Cmp numRel = (Cmp) janusgraphPredicate;
                Preconditions.checkArgument(value instanceof Number);

                switch (numRel) {
                    case EQUAL:
                        return FilterBuilders.inFilter(key, value);
                    case NOT_EQUAL:
                        return FilterBuilders.notFilter(FilterBuilders.inFilter(key, value));
                    case LESS_THAN:
                        return FilterBuilders.rangeFilter(key).lt(value);
                    case LESS_THAN_EQUAL:
                        return FilterBuilders.rangeFilter(key).lte(value);
                    case GREATER_THAN:
                        return FilterBuilders.rangeFilter(key).gt(value);
                    case GREATER_THAN_EQUAL:
                        return FilterBuilders.rangeFilter(key).gte(value);
                    default:
                        throw new IllegalArgumentException("Unexpected relation: " + numRel);
                }
            } else if (value instanceof String) {
                Mapping map = getStringMapping(informations.get(key));
                String fieldName = key;
                if (map==Mapping.TEXT && !Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + janusgraphPredicate);
                if (map==Mapping.STRING && Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + janusgraphPredicate);
                if (map==Mapping.TEXTSTRING && !Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    fieldName = getDualMappingName(key);

                if (janusgraphPredicate == Text.CONTAINS) {
                    value = ((String) value).toLowerCase();
                    AndFilterBuilder b = FilterBuilders.andFilter();
                    for (String term : Text.tokenize((String)value)) {
                        b.add(FilterBuilders.termFilter(fieldName, term));
                    }
                    return b;
                } else if (janusgraphPredicate == Text.CONTAINS_PREFIX) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.prefixFilter(fieldName, (String) value);
                } else if (janusgraphPredicate == Text.CONTAINS_REGEX) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.regexpFilter(fieldName, (String) value);
                } else if (janusgraphPredicate == Text.PREFIX) {
                    return FilterBuilders.prefixFilter(fieldName, (String) value);
                } else if (janusgraphPredicate == Text.REGEX) {
                    return FilterBuilders.regexpFilter(fieldName, (String) value);
                } else if (janusgraphPredicate == Cmp.EQUAL) {
                    return FilterBuilders.termFilter(fieldName, (String) value);
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return FilterBuilders.notFilter(FilterBuilders.termFilter(fieldName, (String) value));
                } else
                    throw new IllegalArgumentException("Predicate is not supported for string value: " + janusgraphPredicate);
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(janusgraphPredicate == Geo.WITHIN, "Relation is not supported for geo value: " + janusgraphPredicate);
                Geoshape shape = (Geoshape) value;
                if (shape.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = shape.getPoint();
                    return FilterBuilders.geoDistanceFilter(key).lat(center.getLatitude()).lon(center.getLongitude()).distance(shape.getRadius(), DistanceUnit.KILOMETERS);
                } else if (shape.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = shape.getPoint(0);
                    Geoshape.Point northeast = shape.getPoint(1);
                    return FilterBuilders.geoBoundingBoxFilter(key).bottomRight(southwest.getLatitude(), northeast.getLongitude()).topLeft(northeast.getLatitude(), southwest.getLongitude());
                } else
                    throw new IllegalArgumentException("Unsupported or invalid search shape type: " + shape.getType());
            } else if (value instanceof Date || value instanceof Instant) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on date types: " + janusgraphPredicate);
                Cmp numRel = (Cmp) janusgraphPredicate;

                switch (numRel) {
                    case EQUAL:
                        return FilterBuilders.inFilter(key, value);
                    case NOT_EQUAL:
                        return FilterBuilders.notFilter(FilterBuilders.inFilter(key, value));
                    case LESS_THAN:
                        return FilterBuilders.rangeFilter(key).lt(value);
                    case LESS_THAN_EQUAL:
                        return FilterBuilders.rangeFilter(key).lte(value);
                    case GREATER_THAN:
                        return FilterBuilders.rangeFilter(key).gt(value);
                    case GREATER_THAN_EQUAL:
                        return FilterBuilders.rangeFilter(key).gte(value);
                    default:
                        throw new IllegalArgumentException("Unexpected relation: " + numRel);
                }
            } else if (value instanceof Boolean) {
                Cmp numRel = (Cmp) janusgraphPredicate;
                switch (numRel) {
                    case EQUAL:
                        return FilterBuilders.inFilter(key, value);
                    case NOT_EQUAL:
                        return FilterBuilders.notFilter(FilterBuilders.inFilter(key, value));
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }

            } else if (value instanceof UUID) {
                if (janusgraphPredicate == Cmp.EQUAL) {
                    return FilterBuilders.termFilter(key, value);
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return FilterBuilders.notFilter(FilterBuilders.termFilter(key, value));
                } else {
                    throw new IllegalArgumentException("Only equal or not equal is supported for UUIDs: " + janusgraphPredicate);
                }
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        } else if (condition instanceof Not) {
            return FilterBuilders.notFilter(getFilter(((Not) condition).getChild(),informations));
        } else if (condition instanceof And) {
            AndFilterBuilder b = FilterBuilders.andFilter();
            for (Condition c : condition.getChildren()) {
                b.add(getFilter(c,informations));
            }
            return b;
        } else if (condition instanceof Or) {
            OrFilterBuilder b = FilterBuilders.orFilter();
            for (Condition c : condition.getChildren()) {
                b.add(getFilter(c,informations));
            }
            return b;
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        SearchRequestBuilder srb = client.prepareSearch(indexName);
        srb.setTypes(query.getStore());
        srb.setQuery(QueryBuilders.matchAllQuery());
        srb.setPostFilter(getFilter(query.getCondition(),informations.get(query.getStore())));
        if (!query.getOrder().isEmpty()) {
            List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (int i = 0; i < orders.size(); i++) {
                IndexQuery.OrderEntry orderEntry = orders.get(i);
                FieldSortBuilder fsb = new FieldSortBuilder(orders.get(i).getKey())
                        .order(orderEntry.getOrder() == Order.ASC ? SortOrder.ASC : SortOrder.DESC);
                if (useDeprecatedIgnoreUnmapped) {
                    fsb.ignoreUnmapped(true);
                } else {
                    Class<?> datatype = orderEntry.getDatatype();
                    fsb.unmappedType(convertToEsDataType(datatype));
                }
                srb.addSort(fsb);
            }
        }
        srb.setFrom(0);
        if (query.hasLimit()) srb.setSize(query.getLimit());
        else srb.setSize(maxResultsSize);
        srb.setNoFields();
        //srb.setExplain(true);

        SearchResponse response = srb.execute().actionGet();
        log.debug("Executed query [{}] in {} ms", query.getCondition(), response.getTookInMillis());
        SearchHits hits = response.getHits();
        if (!query.hasLimit() && hits.totalHits() >= maxResultsSize)
            log.warn("Query result set truncated to first [{}] elements for query: {}", maxResultsSize, query);
        List<String> result = new ArrayList<String>(hits.hits().length);
        for (SearchHit hit : hits) {
            result.add(hit.id());
        }
        return result;
    }

    private String convertToEsDataType(Class<?> datatype) {
        if(String.class.isAssignableFrom(datatype)) {
            return "string";
        }
        else if (Integer.class.isAssignableFrom(datatype)) {
            return "integer";
        }
        else if (Long.class.isAssignableFrom(datatype)) {
            return "long";
        }
        else if (Float.class.isAssignableFrom(datatype)) {
            return "float";
        }
        else if (Double.class.isAssignableFrom(datatype)) {
            return "double";
        }
        else if (Boolean.class.isAssignableFrom(datatype)) {
            return "boolean";
        }
        else if (Date.class.isAssignableFrom(datatype)) {
            return "date";
        }
        else if (Instant.class.isAssignableFrom(datatype)) {
            return "date";
        }
        else if (Geoshape.class.isAssignableFrom(datatype)) {
            return "geo_point";
        }

        return null;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        SearchRequestBuilder srb = client.prepareSearch(indexName);
        srb.setTypes(query.getStore());
        srb.setQuery(QueryBuilders.queryStringQuery(query.getQuery()));

        srb.setFrom(query.getOffset());
        if (query.hasLimit()) srb.setSize(query.getLimit());
        else srb.setSize(maxResultsSize);
        srb.setNoFields();
        //srb.setExplain(true);

        SearchResponse response = srb.execute().actionGet();
        log.debug("Executed query [{}] in {} ms", query.getQuery(), response.getTookInMillis());
        SearchHits hits = response.getHits();
        if (!query.hasLimit() && hits.totalHits() >= maxResultsSize)
            log.warn("Query result set truncated to first [{}] elements for query: {}", maxResultsSize, query);
        List<RawQuery.Result<String>> result = new ArrayList<RawQuery.Result<String>>(hits.hits().length);
        for (SearchHit hit : hits) {
            result.add(new RawQuery.Result<String>(hit.id(),hit.getScore()));
        }
        return result;
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtil.isString(dataType)) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            if (janusgraphPredicate instanceof Cmp) return true;
        } else if (dataType == Geoshape.class) {
            return janusgraphPredicate == Geo.WITHIN;
        } else if (AttributeUtil.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.CONTAINS_REGEX;
                case STRING:
                    return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL || janusgraphPredicate==Text.REGEX || janusgraphPredicate==Text.PREFIX;
                case TEXTSTRING:
                    return (janusgraphPredicate instanceof Text) || janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL;
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            if (janusgraphPredicate instanceof Cmp) return true;
        } else if (dataType == Boolean.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        } else if (dataType == UUID.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL;
        }
        return false;
    }


    @Override
    public boolean supports(KeyInformation information) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class || dataType == Date.class || dataType== Instant.class || dataType == Boolean.class || dataType == UUID.class) {
            if (mapping==Mapping.DEFAULT) return true;
        } else if (AttributeUtil.isString(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.STRING
                    || mapping==Mapping.TEXT || mapping==Mapping.TEXTSTRING) return true;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        Preconditions.checkArgument(!StringUtils.containsAny(key,new char[]{' '}),"Invalid key name provided: %s",key);
        return key;
    }

    @Override
    public IndexFeatures getFeatures() {
        return ES_FEATURES;
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new DefaultTransaction(config);
    }

    @Override
    public void close() throws BackendException {

        if (node != null && !node.isClosed()) {
            node.close();
        }
        client.close();

    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            try {
                client.admin().indices()
                        .delete(new DeleteIndexRequest(indexName)).actionGet();
                // We wait for one second to let ES delete the river
                Thread.sleep(1000);
            } catch (IndexMissingException e) {
                // Index does not exist... Fine
            }
        } catch (Exception e) {
            throw new PermanentBackendException("Could not delete index " + indexName, e);
        } finally {
            close();
        }
    }

    /**
     * Exposed for testing
     */
    Node getNode() {
        return node;
    }

    private void checkExpectedClientVersion() {
        /*
         * This is enclosed in a catch block to prevent an unchecked exception
         * from killing the startup thread.  This check is just advisory -- the
         * most it does is log a warning -- so there's no reason to allow it to
         * emit a exception and potentially block graph startup.
         */
        try {
            if (!Version.CURRENT.toString().equals(ElasticSearchConstants.ES_VERSION_EXPECTED)) {
                log.warn("ES client version ({}) does not match the version with which JanusGraph was compiled ({}).  This might cause problems.",
                        Version.CURRENT, ElasticSearchConstants.ES_VERSION_EXPECTED);
            } else {
                log.debug("Found ES client version matching JanusGraph's compile-time version: {} (OK)", Version.CURRENT);
            }
        } catch (RuntimeException e) {
            log.warn("Unable to check expected ES client version", e);
        }
    }
}
