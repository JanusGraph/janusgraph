package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.util.DefaultTransaction;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;

import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@PreInitializeConfigOptions
public class ElasticSearchIndex implements IndexProvider {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchIndex.class);

    private static final String TTL_FIELD = "_ttl";
    private static final String STRING_MAPPING_SUFFIX = "$STRING";

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

    public static final ConfigOption<ElasticSearchSetup> INTERFACE =
            new ConfigOption<ElasticSearchSetup>(ELASTICSEARCH_NS, "interface",
            "Whether to connect to ES using the Node or Transport client (see the \"Talking to Elasticsearch\" " +
            "section of the ES manual for discussion of the difference).  Setting this option enables the " +
            "interface config track (see manual for more information about ES config tracks).",
            ConfigOption.Type.MASKABLE, ElasticSearchSetup.class, ElasticSearchSetup.TRANSPORT_CLIENT);

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
            "When Titan initializes its ES backend, Titan waits up to this duration for the " +
            "ES cluster health to reach at least yellow status.  " +
            "This string should be formatted as a natural number followed by the lowercase letter " +
            "\"s\", e.g. 3s or 60s.", ConfigOption.Type.MASKABLE, "30s");

    public static final ConfigOption<Boolean> LOAD_DEFAULT_NODE_SETTINGS =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "load-default-node-settings",
            "Whether ES's Node client will internally attempt to load default configuration settings " +
            "from system properties/process environment variables.  Only meaningful when using the Node " +
            "client (has no effect with TransportClient).", ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace ES_EXTRAS_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "ext", "Overrides for arbitrary elasticsearch.yaml settings", true);

    public static final ConfigNamespace ES_CREATE_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "create", "Settings related to index creation");

    public static final ConfigOption<Long> CREATE_SLEEP =
            new ConfigOption<Long>(ES_CREATE_NS, "sleep",
            "How long to sleep, in milliseconds, between the successful completion of a (blocking) index " +
            "creation request and the first use of that index.  This only applies when creating an index in ES, " +
            "which typically only happens the first time Titan is started on top of ES. If the index Titan is " +
            "configured to use already exists, then this setting has no effect.", ConfigOption.Type.MASKABLE, 200L);

    public static final ConfigNamespace ES_CREATE_EXTRAS_NS =
            new ConfigNamespace(ES_CREATE_NS, "ext", "Overrides for arbitrary settings applied at index creation", true);

    private static final IndexFeatures ES_FEATURES = new IndexFeatures.Builder().supportsDocumentTTL()
            .setDefaultStringMapping(Mapping.TEXT).supportedStringMappings(Mapping.TEXT, Mapping.TEXTSTRING, Mapping.STRING).setWildcardField("_all").build();

    public static final int HOST_PORT_DEFAULT = 9300;

    private final Node node;
    private final Client client;
    private final String indexName;
    private final int maxResultsSize;

    public ElasticSearchIndex(Configuration config) {
        indexName = config.get(INDEX_NAME);

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

            ElasticSearchSetup.applySettingsFromTitanConf(settings, config, ES_CREATE_EXTRAS_NS);

            CreateIndexResponse create = client.admin().indices().prepareCreate(indexName)
                    .setSettings(settings.build()).execute().actionGet();
            try {
                final long sleep = config.get(CREATE_SLEEP);
                log.debug("Sleeping {} ms after {} index creation returned from actionGet()", sleep, indexName);
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                throw new TitanException("Interrupted while waiting for index to settle in", e);
            }
            if (!create.isAcknowledged()) throw new IllegalArgumentException("Could not create index: " + indexName);
        }
    }


    /**
     * Configure ElasticSearchIndex's ES client according to semantics introduced in
     * 0.5.1.  Allows greater flexibility than the previous config semantics.  See
     * {@link com.thinkaurelius.titan.diskstorage.es.ElasticSearchSetup} for more
     * information.
     * <p>
     * This is activated by setting an explicit value for {@link #INTERFACE} in
     * the Titan configuration.
     *
     * @see #legacyConfiguration(com.thinkaurelius.titan.diskstorage.configuration.Configuration)
     * @param config a config passed to ElasticSearchIndex's constructor
     * @return a node and client object open and ready for use
     */
    private ElasticSearchSetup.Connection interfaceConfiguration(Configuration config) {
        ElasticSearchSetup clientMode = config.get(INTERFACE);

        try {
            return clientMode.connect(config);
        } catch (IOException e) {
            throw new TitanException(e);
        }
    }

    /**
     * Configure ElasticSearchIndex's ES client according to 0.4.x - 0.5.0 semantics.
     * This checks local-mode first.  If local-mode is true, then it creates a Node that
     * uses JVM local transport and can't talk over the network.  If local-mode is
     * false, then it creates a TransportClient that can talk over the network and
     * uses {@link com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}
     * as the server addresses.  Note that this configuration method
     * does not allow creating a Node that talks over the network.
     * <p>
     * This is activated by <b>not</b> setting an explicit value for {@link #INTERFACE} in the
     * Titan configuration.
     *
     * @see #interfaceConfiguration(com.thinkaurelius.titan.diskstorage.configuration.Configuration)
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
                    throw new TitanException(e);
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
            } else if (dataType == Double.class || dataType == Decimal.class || dataType == Precision.class) {
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
            } else if (dataType == Date.class) {
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

    public XContentBuilder getContent(final List<IndexEntry> additions, KeyInformation.StoreRetriever informations, int ttl) throws BackendException {
        Preconditions.checkArgument(ttl>=0);
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            // JSON writes duplicate fields one after another, which forces us
            // at this stage to make de-duplication on the IndexEntry list. We don't want to pay the
            // price map storage on the Mutation level because non of other backends need that.
            Map<String, IndexEntry> uniq = new HashMap<String, IndexEntry>(additions.size()) {{
                for (IndexEntry e : additions)
                    put(e.field, e);
            }};

            for (IndexEntry add : uniq.values()) {
                if (add.value instanceof Number) {
                    if (AttributeUtil.isWholeNumber((Number) add.value)) {
                        builder.field(add.field, ((Number) add.value).longValue());
                    } else { //double or float
                        builder.field(add.field, ((Number) add.value).doubleValue());
                    }
                } else if (AttributeUtil.isString(add.value)) {
                    builder.field(add.field, (String) add.value);
                    if (hasDualStringMapping(informations.get(add.field))) {
                        builder.field(getDualMappingName(add.field), (String) add.value);
                    }
                } else if (add.value instanceof Geoshape) {
                    Geoshape shape = (Geoshape) add.value;
                    if (shape.getType() == Geoshape.Type.POINT) {
                        Geoshape.Point p = shape.getPoint();
                        builder.field(add.field, new double[]{p.getLongitude(), p.getLatitude()});
                    } else throw new UnsupportedOperationException("Geo type is not supported: " + shape.getType());

//                    builder.startObject(add.key);
//                    switch (shape.getType()) {
//                        case POINT:
//                            Geoshape.Point p = shape.getPoint();
//                            builder.field("type","point");
//                            builder.field("coordinates",new double[]{p.getLongitude(),p.getLatitude()});
//                            break;
//                        case BOX:
//                        Geoshape.Point southwest = shape.getPoint(0), northeast = shape.getPoint(1);
//                            builder.field("type","envelope");
//                            builder.field("coordinates",new double[][]{
//                                    {southwest.getLongitude(),northeast.getLatitude()},
//                                    {northeast.getLongitude(),southwest.getLatitude()}});
//                            break;
//                        default: throw new UnsupportedOperationException("Geo type is not supported: " + shape.getType());
//                    }
//                    builder.endObject();
                } else if (add.value instanceof Date) {
                    builder.field(add.field, ((Date) add.value));
                } else if (add.value instanceof Boolean) {
                    builder.field(add.field, ((Boolean) add.value));
                } else if (add.value instanceof UUID) {
                    builder.field(add.field, add.value.toString());
                } else throw new IllegalArgumentException("Unsupported type: " + add.value);

            }
            if (ttl>0) builder.field(TTL_FIELD, TimeUnit.MILLISECONDS.convert(ttl,TimeUnit.SECONDS));

            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new PermanentBackendException("Could not write json");
        }
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
                            StringBuilder script = new StringBuilder();
                            for (String key : Iterables.transform(mutation.getDeletions(),IndexMutation.ENTRY2FIELD_FCT)) {
                                script.append("ctx._source.remove(\"" + key + "\"); ");
                                if (hasDualStringMapping(informations.get(storename,key))) {
                                    script.append("ctx._source.remove(\"" + getDualMappingName(key) + "\"); ");
                                }
                                log.trace("Deleting individual field [{}] for document {}", key, docid);
                            }
                            brb.add(client.prepareUpdate(indexName, storename, docid).setScript(script.toString()));
                            bulkrequests++;
                        }

                        bulkrequests++;
                    }
                    if (mutation.hasAdditions()) {
                        int ttl = mutation.determineTTL();

                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", docid);
                            brb.add(new IndexRequest(indexName, storename, docid)
                                    .source(getContent(mutation.getAdditions(), informations.get(storename), ttl)));
                        } else {
                            Preconditions.checkArgument(ttl==0,"Elasticsearch only supports TTL on new documents [%s]",docid);
                            boolean needUpsert = !mutation.hasDeletions();
                            XContentBuilder builder = getContent(mutation.getAdditions(),informations.get(storename),ttl);
                            UpdateRequestBuilder update = client.prepareUpdate(indexName, storename, docid).setDoc(builder);
                            if (needUpsert) update.setUpsert(builder);
                            log.trace("Updating document {} with upsert {}", docid, needUpsert);
                            brb.add(update);
                            bulkrequests++;
                        }

                        bulkrequests++;
                    }

                }
            }
            if (bulkrequests > 0) brb.execute().actionGet();
        } catch (Exception e) {
            throw convert(e);
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
                        bulk.add(new IndexRequest(indexName, store, docID).source(getContent(content, informations.get(store), IndexMutation.determineTTL(content))));
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
            TitanPredicate titanPredicate = atom.getPredicate();
            if (value instanceof Number) {
                Preconditions.checkArgument(titanPredicate instanceof Cmp, "Relation not supported on numeric types: " + titanPredicate);
                Cmp numRel = (Cmp) titanPredicate;
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
                if (map==Mapping.TEXT && !titanPredicate.toString().startsWith("CONTAINS"))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + titanPredicate);
                if (map==Mapping.STRING && titanPredicate.toString().startsWith("CONTAINS"))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + titanPredicate);
                if (map==Mapping.TEXTSTRING && !titanPredicate.toString().startsWith("CONTAINS"))
                    fieldName = getDualMappingName(key);

                if (titanPredicate == Text.CONTAINS) {
                    value = ((String) value).toLowerCase();
                    AndFilterBuilder b = FilterBuilders.andFilter();
                    for (String term : Text.tokenize((String)value)) {
                        b.add(FilterBuilders.termFilter(fieldName, term));
                    }
                    return b;
                } else if (titanPredicate == Text.CONTAINS_PREFIX) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.prefixFilter(fieldName, (String) value);
                } else if (titanPredicate == Text.CONTAINS_REGEX) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.regexpFilter(fieldName, (String) value);
                } else if (titanPredicate == Text.PREFIX) {
                    return FilterBuilders.prefixFilter(fieldName, (String) value);
                } else if (titanPredicate == Text.REGEX) {
                    return FilterBuilders.regexpFilter(fieldName, (String) value);
                } else if (titanPredicate == Cmp.EQUAL) {
                    return FilterBuilders.termFilter(fieldName, (String) value);
                } else if (titanPredicate == Cmp.NOT_EQUAL) {
                    return FilterBuilders.notFilter(FilterBuilders.termFilter(fieldName, (String) value));
                } else
                    throw new IllegalArgumentException("Predicate is not supported for string value: " + titanPredicate);
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(titanPredicate == Geo.WITHIN, "Relation is not supported for geo value: " + titanPredicate);
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
            } else if (value instanceof Date) {
                Preconditions.checkArgument(titanPredicate instanceof Cmp, "Relation not supported on date types: " + titanPredicate);
                Cmp numRel = (Cmp) titanPredicate;

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
                Cmp numRel = (Cmp) titanPredicate;
                switch (numRel) {
                    case EQUAL:
                        return FilterBuilders.inFilter(key, value);
                    case NOT_EQUAL:
                        return FilterBuilders.notFilter(FilterBuilders.inFilter(key, value));
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }

            } else if (value instanceof UUID) {
                if (titanPredicate == Cmp.EQUAL) {
                    return FilterBuilders.termFilter(key, value);
                } else if (titanPredicate == Cmp.NOT_EQUAL) {
                    return FilterBuilders.notFilter(FilterBuilders.termFilter(key, value));
                } else {
                    throw new IllegalArgumentException("Only equal or not equal is supported for UUIDs: " + titanPredicate);
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
                srb.addSort(new FieldSortBuilder(orders.get(i).getKey())
                        .order(orders.get(i).getOrder() == Order.ASC ? SortOrder.ASC : SortOrder.DESC)
                        .ignoreUnmapped(true));
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

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        SearchRequestBuilder srb = client.prepareSearch(indexName);
        srb.setTypes(query.getStore());
        srb.setQuery(QueryBuilders.queryString(query.getQuery()));

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
    public boolean supports(KeyInformation information, TitanPredicate titanPredicate) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtil.isString(dataType)) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            if (titanPredicate instanceof Cmp) return true;
        } else if (dataType == Geoshape.class) {
            return titanPredicate == Geo.WITHIN;
        } else if (AttributeUtil.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return titanPredicate == Text.CONTAINS || titanPredicate == Text.CONTAINS_PREFIX || titanPredicate == Text.CONTAINS_REGEX;
                case STRING:
                    return titanPredicate == Cmp.EQUAL || titanPredicate==Cmp.NOT_EQUAL || titanPredicate==Text.REGEX || titanPredicate==Text.PREFIX;
                case TEXTSTRING:
                    return (titanPredicate instanceof Text) || titanPredicate == Cmp.EQUAL || titanPredicate==Cmp.NOT_EQUAL;
            }
        } else if (dataType == Date.class) {
            if (titanPredicate instanceof Cmp) return true;
        } else if (dataType == Boolean.class) {
            return titanPredicate == Cmp.EQUAL || titanPredicate == Cmp.NOT_EQUAL;
        } else if (dataType == UUID.class) {
            return titanPredicate == Cmp.EQUAL || titanPredicate==Cmp.NOT_EQUAL;
        }
        return false;
    }


    @Override
    public boolean supports(KeyInformation information) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class || dataType == Date.class || dataType == Boolean.class || dataType == UUID.class) {
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
        client.close();
        if (node != null && !node.isClosed()) {
            node.close();
        }
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
                log.warn("ES client version ({}) does not match the version with which Titan was compiled ({}).  This might cause problems.",
                        Version.CURRENT, ElasticSearchConstants.ES_VERSION_EXPECTED);
            } else {
                log.debug("Found ES client version matching Titan's compile-time version: {} (OK)", Version.CURRENT);
            }
        } catch (RuntimeException e) {
            log.warn("Unable to check expected ES client version", e);
        }
    }
}
