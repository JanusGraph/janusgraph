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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.locationtech.spatial4j.shape.Rectangle;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectWriter;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_DOC_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_GEO_COORDS_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_TYPE_KEY;

import org.janusgraph.diskstorage.es.compat.AbstractESCompat;
import org.janusgraph.diskstorage.es.compat.ES1Compat;
import org.janusgraph.diskstorage.es.compat.ES2Compat;
import org.janusgraph.diskstorage.es.compat.ES5Compat;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@PreInitializeConfigOptions
public class ElasticSearchIndex implements IndexProvider {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchIndex.class);

    private static final String STRING_MAPPING_SUFFIX = "__STRING";

    public static final ConfigNamespace ELASTICSEARCH_NS =
            new ConfigNamespace(INDEX_NS, "elasticsearch", "Elasticsearch index configuration");

    public static final ConfigOption<String> CLUSTER_NAME =
            new ConfigOption<String>(ELASTICSEARCH_NS, "cluster-name",
            "The name of the Elasticsearch cluster.  This should match the \"cluster.name\" setting " +
            "in the Elasticsearch nodes' configuration.", ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");

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
            ConfigOption.Type.MASKABLE, String.class, ElasticSearchSetup.REST_CLIENT.toString(),
            disallowEmpty(String.class));

    public static final ConfigOption<Boolean> IGNORE_CLUSTER_NAME =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "ignore-cluster-name",
            "Whether to bypass validation of the cluster name of connected nodes.  " +
            "This option is only used on the interface configuration track (see manual for " +
            "information about ES config tracks).", ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String> HEALTH_REQUEST_TIMEOUT =
            new ConfigOption<String>(ELASTICSEARCH_NS, "health-request-timeout",
            "When JanusGraph initializes its ES backend, JanusGraph waits up to this duration for the " +
            "ES cluster health to reach at least yellow status.  " +
            "This string should be formatted as a natural number followed by the lowercase letter " +
            "\"s\", e.g. 3s or 60s.", ConfigOption.Type.MASKABLE, "30s");

    public static final ConfigOption<String> BULK_REFRESH =
            new ConfigOption<String>(ELASTICSEARCH_NS, "bulk-refresh",
            "Elasticsearch bulk API refresh setting used to control when changes made by this request are made " +
            "visible to search", ConfigOption.Type.MASKABLE, "false");

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

    public static final ConfigOption<Boolean> USE_EXTERNAL_MAPPINGS =
            new ConfigOption<Boolean>(ES_CREATE_NS, "use-external-mappings",
            "Whether JanusGraph should make use of an external mapping when registering an index.", ConfigOption.Type.MASKABLE, false);

    private static final IndexFeatures ES_FEATURES = new IndexFeatures.Builder()
            .setDefaultStringMapping(Mapping.TEXT).supportedStringMappings(Mapping.TEXT, Mapping.TEXTSTRING, Mapping.STRING).setWildcardField("_all").supportsCardinality(Cardinality.SINGLE).supportsCardinality(Cardinality.LIST).supportsCardinality(Cardinality.SET).supportsNanoseconds().supportsCustomAnalyzer().build();

    public static final int HOST_PORT_DEFAULT = 9200;

    /**
     * Default tree_levels used when creating geo_shape mappings.
     */
    public static final int DEFAULT_GEO_MAX_LEVELS = 20;

    /**
     * Default distance_error_pct used when creating geo_shape mappings.
     */
    public static final double DEFAULT_GEO_DIST_ERROR_PCT = 0.025;

    private static final ObjectWriter mapWriter;
    static {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapWriter = mapper.writerWithView(Map.class);
    }

    private final AbstractESCompat compat;
    private final ElasticSearchClient client;
    private final String indexName;
    private final int maxResultsSize;
    private final boolean useExternalMappings;

    public ElasticSearchIndex(Configuration config) throws BackendException {
        indexName = config.get(INDEX_NAME);
        useExternalMappings = config.get(USE_EXTERNAL_MAPPINGS);

        final ElasticSearchSetup.Connection c = interfaceConfiguration(config);
        client = c.getClient();

        maxResultsSize = config.get(INDEX_MAX_RESULT_SET_SIZE);
        log.debug("Configured ES query result set max size to {}", maxResultsSize);

        switch (client.getMajorVersion()) {
            case ONE:
                compat = new ES1Compat();
                break;
            case TWO:
                compat = new ES2Compat();
                break;
            case FIVE:
                compat = new ES5Compat();
                break;
            default:
                throw new PermanentBackendException("Unsupported Elasticsearch version: " + client.getMajorVersion());
        }

        try {
            client.clusterHealthRequest(config.get(HEALTH_REQUEST_TIMEOUT));
            checkForOrCreateIndex(config);
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }

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
     * @throws IOException if the index status could not be checked or index could not be created
     */
    private void checkForOrCreateIndex(Configuration config) throws IOException {
        Preconditions.checkState(null != client);

        //Create index if it does not useExternalMappings and if it does not already exist
        if (!useExternalMappings && !client.indexExists(indexName)) {
            final Map<String,Object> settings = new HashMap<>();

            ElasticSearchSetup.applySettingsFromJanusGraphConf(settings, config, ES_CREATE_EXTRAS_NS);
            settings.put("index.max_result_window", Integer.MAX_VALUE);
            client.createIndex(indexName, settings);

            try {
                final long sleep = config.get(CREATE_SLEEP);
                log.debug("Sleeping {} ms after {} index creation returned from actionGet()", sleep, indexName);
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                throw new JanusGraphException("Interrupted while waiting for index to settle in", e);
            }
        }
        if (!client.indexExists(indexName)) throw new IllegalArgumentException("Could not create index: " + indexName);
    }


    /**
     * Configure ElasticSearchIndex's ES client. See{@link org.janusgraph.diskstorage.es.ElasticSearchSetup} for more
     * information.
     *
     * @param config a config passed to ElasticSearchIndex's constructor
     * @return a client object open and ready for use
     */
    private ElasticSearchSetup.Connection interfaceConfiguration(Configuration config) {
        ElasticSearchSetup clientMode = ConfigOption.getEnumValue(config.get(INTERFACE), ElasticSearchSetup.class);

        try {
            return clientMode.connect(config);
        } catch (IOException e) {
            throw new JanusGraphException(e);
        }
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
        Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map==Mapping.DEFAULT || AttributeUtil.isString(dataType) ||
                (map==Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType)),
                "Specified illegal mapping [%s] for data type [%s]",map,dataType);

        if (useExternalMappings) {
            try {
                //We check if the externalMapping have the property 'key'
                Map mappings = client.getMapping(indexName, store);
                if (!mappings.containsKey(key)) {
                    throw new PermanentBackendException("The external mapping for index '"+indexName+"' and type '"+store+"' do not have property '"+key+"'");
                }
            } catch (IOException e) {
                throw new PermanentBackendException(e);
            }
        } else {
            this.pushMapping(store, key, information);
        }
    }

    /**
     * Push mapping to ElasticSearch
     * @param store the type in the index
     * @param key the name of the property in the index
     * @param information information of the key
     */
    private void pushMapping(String store, String key, KeyInformation information) throws AssertionError, PermanentBackendException, BackendException {
        Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        final Map<String,Object> properties = new HashMap<>();
        if (AttributeUtil.isString(dataType)) {
            if (map==Mapping.DEFAULT) map=Mapping.TEXT;
            log.debug("Registering string type for {} with mapping {}", key, map);
            String stringAnalyzer = (String) ParameterType.STRING_ANALYZER.findParameter(information.getParameters(), null);
            String textAnalyzer = (String) ParameterType.TEXT_ANALYZER.findParameter(information.getParameters(), null);
            // use keyword type for string mappings unless custom string analyzer is provided
            final Map<String,Object> stringMapping = stringAnalyzer == null ? compat.createKeywordMapping() : compat.createTextMapping(stringAnalyzer);
            switch (map) {
                case STRING:
                    properties.put(key, stringMapping);
                    break;
                case TEXT:
                    properties.put(key, compat.createTextMapping(textAnalyzer));
                    break;
                case TEXTSTRING:
                    properties.put(key, compat.createTextMapping(textAnalyzer));
                    properties.put(getDualMappingName(key), stringMapping);
                    break;
                default: throw new AssertionError("Unexpected mapping: "+map);
            }
        } else if (dataType == Float.class) {
            log.debug("Registering float type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "float"));
        } else if (dataType == Double.class) {
            log.debug("Registering double type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "double"));
        } else if (dataType == Byte.class) {
            log.debug("Registering byte type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "byte"));
        } else if (dataType == Short.class) {
            log.debug("Registering short type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "short"));
        } else if (dataType == Integer.class) {
            log.debug("Registering integer type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "integer"));
        } else if (dataType == Long.class) {
            log.debug("Registering long type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "long"));
        } else if (dataType == Boolean.class) {
            log.debug("Registering boolean type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "boolean"));
        } else if (dataType == Geoshape.class) {
            switch (map) {
                case PREFIX_TREE:
                    int maxLevels = (int) ParameterType.INDEX_GEO_MAX_LEVELS.findParameter(information.getParameters(), DEFAULT_GEO_MAX_LEVELS);
                    double distErrorPct = (double) ParameterType.INDEX_GEO_DIST_ERROR_PCT.findParameter(information.getParameters(), DEFAULT_GEO_DIST_ERROR_PCT);
                    log.debug("Registering geo_shape type for {} with tree_levels={} and distance_error_pct={}", key, maxLevels, distErrorPct);
                    properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "geo_shape",
                        "tree", "quadtree",
                        "tree_levels", maxLevels,
                        "distance_error_pct", distErrorPct));
                    break;
                default:
                    log.debug("Registering geo_point type for {}", key);
                    properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "geo_point"));
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            log.debug("Registering date type for {}", key);
            properties.put(key, ImmutableMap.of(ES_TYPE_KEY, "date"));
        } else if (dataType == UUID.class) {
            log.debug("Registering uuid type for {}", key);
            properties.put(key, compat.createKeywordMapping());
        }

        final Map<String,Object> mapping = ImmutableMap.of("properties", properties);

        try {
            client.createMapping(indexName, store, mapping);
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

    public Map<String, Object> getNewDocument(final List<IndexEntry> additions, KeyInformation.StoreRetriever informations) throws BackendException {
        // JSON writes duplicate fields one after another, which forces us
        // at this stage to make de-duplication on the IndexEntry list. We don't want to pay the
        // price map storage on the Mutation level because none of other backends need that.

        Multimap<String, IndexEntry> uniq = LinkedListMultimap.create();
        for (IndexEntry e : additions) {
            uniq.put(e.field, e);
        }

        final Map<String, Object> doc = new HashMap<>();
        for (Map.Entry<String, Collection<IndexEntry>> add : uniq.asMap().entrySet()) {
            KeyInformation keyInformation = informations.get(add.getKey());
            Object value = null;
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    value = convertToEsType(Iterators.getLast(add.getValue().iterator()).value, Mapping.getMapping(keyInformation));
                    break;
                case SET:
                case LIST:
                    value = add.getValue().stream().map(v -> convertToEsType(v.value, Mapping.getMapping(keyInformation)))
                        .filter(v -> {
                            Preconditions.checkArgument(!(v instanceof byte[]), "Collections not supported for " + add.getKey());
                            return true;
                        })
                        .collect(Collectors.toList()).toArray();
                    break;
            }

            doc.put(add.getKey(), value);
            if (hasDualStringMapping(informations.get(add.getKey())) && keyInformation.getDataType() == String.class) {
                doc.put(getDualMappingName(add.getKey()), value);
            }


        }

        return doc;
    }

    private static Object convertToEsType(Object value, Mapping mapping) {
        if (value instanceof Number) {
            if (AttributeUtil.isWholeNumber((Number) value)) {
                return ((Number) value).longValue();
            } else { //double or float
                return ((Number) value).doubleValue();
            }
        } else if (AttributeUtil.isString(value)) {
            return value;
        } else if (value instanceof Geoshape) {
            return convertgeo((Geoshape) value, mapping);
        } else if (value instanceof Date) {
            return value;
        } else if (value instanceof  Instant) {
            return Date.from((Instant) value);
        } else if (value instanceof Boolean) {
            return value;
        } else if (value instanceof UUID) {
            return value.toString();
        } else throw new IllegalArgumentException("Unsupported type: " + value.getClass() + " (value: " + value + ")");
    }

    @SuppressWarnings("unchecked")
    private static Object convertgeo(Geoshape geoshape, Mapping mapping) {
        if (geoshape.getType() == Geoshape.Type.POINT && Mapping.PREFIX_TREE != mapping) {
            Geoshape.Point p = geoshape.getPoint();
            return new double[]{p.getLongitude(), p.getLatitude()};
        } else if (geoshape.getType() == Geoshape.Type.BOX) {
            Rectangle box = geoshape.getShape().getBoundingBox();
            Map<String,Object> map = new HashMap<>();
            map.put("type", "envelope");
            map.put("coordinates", new double[][] {{box.getMinX(),box.getMaxY()},{box.getMaxX(),box.getMinY()}});
            return map;
        } else if (geoshape.getType() == Geoshape.Type.CIRCLE) {
            try {
                Map<String,Object> map = geoshape.toMap();
                map.put("radius", map.get("radius") + ((Map<String, String>) map.remove("properties")).get("radius_units"));
                return map;
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
            }
        } else {
            try {
                return geoshape.toMap();
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
            }
        }
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
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
                            requests.add(ElasticSearchMutation.createDeleteRequest(indexName, storename, docid));
                        } else {
                            String script = getDeletionScript(informations, storename, mutation);
                            Map<String,Object> doc = compat.prepareScript(script).build();
                            requests.add(ElasticSearchMutation.createUpdateRequest(indexName, storename, docid, doc));
                            log.trace("Adding script {}", script);
                        }
                    }
                    if (mutation.hasAdditions()) {
                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", docid);
                            Map<String, Object> source = getNewDocument(mutation.getAdditions(), informations.get(storename));
                            requests.add(ElasticSearchMutation.createIndexRequest(indexName, storename, docid, source));
                        } else {
                            final Map upsert;
                            if (!mutation.hasDeletions()) {
                                upsert = getNewDocument(mutation.getAdditions(), informations.get(storename));
                            } else {
                                upsert = null;
                            }

                            String inline = getAdditionScript(informations, storename, mutation);
                            if (!inline.isEmpty()) {
                                final ImmutableMap.Builder builder = compat.prepareScript(inline);
                                requests.add(ElasticSearchMutation.createUpdateRequest(indexName, storename, docid, builder, upsert));
                                log.trace("Adding script {}", inline);
                            }

                            Map<String,Object> doc = getAdditionDoc(informations, storename, mutation);
                            if (!doc.isEmpty()) {
                                final ImmutableMap.Builder builder = ImmutableMap.builder().put(ES_DOC_KEY, doc);
                                requests.add(ElasticSearchMutation.createUpdateRequest(indexName, storename, docid, builder, upsert));
                                log.trace("Adding update {}", doc);
                            }
                        }
                    }

                }
            }
            if (!requests.isEmpty()) {
                client.bulkRequest(requests);
            }
        } catch (Exception e) {
            log.error("Failed to execute bulk Elasticsearch mutation", e);
            throw convert(e);
        }
    }

    private String getDeletionScript(KeyInformation.IndexRetriever informations, String storename, IndexMutation mutation) throws PermanentBackendException {
        StringBuilder script = new StringBuilder();
        for (IndexEntry deletion : mutation.getDeletions()) {
            KeyInformation keyInformation = informations.get(storename).get(deletion.field);

            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    script.append("ctx._source.remove(\"").append(deletion.field).append("\");");
                    if (hasDualStringMapping(informations.get(storename, deletion.field))) {
                        script.append("ctx._source.remove(\"").append(getDualMappingName(deletion.field)).append("\");");
                    }
                    break;
                case SET:
                case LIST:
                    String jsValue = convertToJsType(deletion.value, compat.scriptLang(), Mapping.getMapping(keyInformation));
                    script.append("def index = ctx._source[\"").append(deletion.field).append("\"].indexOf(").append(jsValue).append("); ctx._source[\"").append(deletion.field).append("\"].remove(index);");
                    if (hasDualStringMapping(informations.get(storename, deletion.field))) {
                        script.append("def index = ctx._source[\"").append(getDualMappingName(deletion.field)).append("\"].indexOf(").append(jsValue).append("); ctx._source[\"").append(getDualMappingName(deletion.field)).append("\"].remove(index);");
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
                case SET:
                case LIST:
                    script.append("ctx._source[\"").append(e.field).append("\"].add(").append(convertToJsType(e.value, compat.scriptLang(), Mapping.getMapping(keyInformation))).append(");");
                    if (hasDualStringMapping(keyInformation)) {
                        script.append("ctx._source[\"").append(getDualMappingName(e.field)).append("\"].add(").append(convertToJsType(e.value, compat.scriptLang(), Mapping.getMapping(keyInformation))).append(");");
                    }
                    break;
                default:
                    break;

            }

        }
        return script.toString();
    }

    private Map<String,Object> getAdditionDoc(KeyInformation.IndexRetriever informations, String storename, IndexMutation mutation) throws PermanentBackendException {
        Map<String,Object> doc = new HashMap<>();
        for (IndexEntry e : mutation.getAdditions()) {
            KeyInformation keyInformation = informations.get(storename).get(e.field);
            if (keyInformation.getCardinality() == Cardinality.SINGLE) {
                doc.put(e.field, convertToEsType(e.value, Mapping.getMapping(keyInformation)));
                if (hasDualStringMapping(keyInformation)) {
                    doc.put(getDualMappingName(e.field), convertToEsType(e.value, Mapping.getMapping(keyInformation)));
                }
            }
        }

        return doc;
    }

    private static String convertToJsType(Object value, String scriptLang, Mapping mapping) throws PermanentBackendException {
        final String esValue;
        try {
            esValue = mapWriter.writeValueAsString(convertToEsType(value, mapping));
        } catch (IOException e) {
            throw new PermanentBackendException("Could not write json");
        }
        return scriptLang.equals("groovy") ? esValue.replace("$", "\\$") : esValue;
    }


    public void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
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

                        requests.add(ElasticSearchMutation.createDeleteRequest(indexName, store, docID));
                    } else {
                        // Add
                        if (log.isTraceEnabled())
                            log.trace("Adding entire document {}", docID);
                        Map<String, Object> source = getNewDocument(content, informations.get(store));
                        requests.add(ElasticSearchMutation.createIndexRequest(indexName, store, docID, source));
                    }
                }
            }

            if (!requests.isEmpty())
                client.bulkRequest(requests);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public Map<String,Object> getFilter(Condition<?> condition, KeyInformation.StoreRetriever informations) {
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
                        return compat.term(key, value);
                    case NOT_EQUAL:
                        return compat.boolMustNot(compat.term(key, value));
                    case LESS_THAN:
                        return compat.lt(key, value);
                    case LESS_THAN_EQUAL:
                        return compat.lte(key, value);
                    case GREATER_THAN:
                        return compat.gt(key, value);
                    case GREATER_THAN_EQUAL:
                        return compat.gte(key, value);
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
                if (map==Mapping.TEXTSTRING && !Text.HAS_CONTAINS.contains(janusgraphPredicate)) {
                    fieldName = getDualMappingName(key);
                } else {
                    fieldName = key;
                }

                if (janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Cmp.EQUAL) {
                    return compat.match(key, value);
                } else if (janusgraphPredicate == Text.CONTAINS_PREFIX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(informations.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.prefix(fieldName, value);
                } else if (janusgraphPredicate == Text.CONTAINS_REGEX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(informations.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.regexp(fieldName, value);
                } else if (janusgraphPredicate == Text.PREFIX) {
                    return compat.prefix(fieldName, value);
                } else if (janusgraphPredicate == Text.REGEX) {
                    return compat.regexp(fieldName, value);
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return compat.boolMustNot(compat.match(fieldName, value));
                } else if (janusgraphPredicate == Text.FUZZY || janusgraphPredicate == Text.CONTAINS_FUZZY) {
                    return compat.fuzzyMatch(fieldName, value);
                } else
                    throw new IllegalArgumentException("Predicate is not supported for string value: " + janusgraphPredicate);
            } else if (value instanceof Geoshape && Mapping.getMapping(informations.get(key)) == Mapping.DEFAULT) {
                // geopoint
                Geoshape shape = (Geoshape) value;
                Preconditions.checkArgument(janusgraphPredicate instanceof Geo && janusgraphPredicate != Geo.CONTAINS, "Relation not supported on geopoint types: " + janusgraphPredicate);

                final Map<String,Object> query;
                if (shape.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = shape.getPoint();
                    query = compat.geoDistance(key, center.getLatitude(), center.getLongitude(), shape.getRadius());
                } else if (shape.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = shape.getPoint(0);
                    Geoshape.Point northeast = shape.getPoint(1);
                    query = compat.geoBoundingBox(key, southwest.getLatitude(), southwest.getLongitude(), northeast.getLatitude(), northeast.getLongitude());
                } else if (shape.getType() == Geoshape.Type.POLYGON) {
                    final List<List<Double>> points = IntStream.range(0, shape.size())
                        .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(), shape.getPoint(i).getLatitude()))
                        .collect(Collectors.toList());
                    query = compat.geoPolygon(key, points);
                } else {
                    throw new IllegalArgumentException("Unsupported or invalid search shape type for geopoint: " + shape.getType());
                }

                return janusgraphPredicate == Geo.DISJOINT ?  compat.boolMustNot(query) : query;
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Geo, "Relation not supported on geoshape types: " + janusgraphPredicate);
                Geoshape shape = (Geoshape) value;
                final Map<String,Object> geo;
                switch (shape.getType()) {
                    case CIRCLE:
                        Geoshape.Point center = shape.getPoint();
                        geo = ImmutableMap.of(ES_TYPE_KEY, "circle",
                            ES_GEO_COORDS_KEY, ImmutableList.of(center.getLongitude(), center.getLatitude()),
                            "radius", shape.getRadius() + "km");
                        break;
                    case BOX:
                        Geoshape.Point southwest = shape.getPoint(0);
                        Geoshape.Point northeast = shape.getPoint(1);
                        geo = ImmutableMap.of(ES_TYPE_KEY, "envelope",
                            ES_GEO_COORDS_KEY, ImmutableList.of(ImmutableList.of(southwest.getLongitude(),northeast.getLatitude()),
                                ImmutableList.of(northeast.getLongitude(),southwest.getLatitude())));
                        break;
                    case LINE:
                        final List lineCoords = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(), shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        geo = ImmutableMap.of(ES_TYPE_KEY, "linestring", ES_GEO_COORDS_KEY, lineCoords);
                        break;
                    case POLYGON:
                        final List polyCoords = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(), shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        geo = ImmutableMap.of(ES_TYPE_KEY, "polygon", ES_GEO_COORDS_KEY, ImmutableList.of(polyCoords));
                        break;
                    case POINT:
                        geo = ImmutableMap.of(ES_TYPE_KEY, "point",
                            ES_GEO_COORDS_KEY, ImmutableList.of(shape.getPoint().getLongitude(),shape.getPoint().getLatitude()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported or invalid search shape type: " + shape.getType());
                }

                return compat.geoShape(key, geo, (Geo) janusgraphPredicate);
            } else if (value instanceof Date || value instanceof Instant) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on date types: " + janusgraphPredicate);
                Cmp numRel = (Cmp) janusgraphPredicate;

                if (value instanceof Instant) {
                    value = Date.from((Instant) value);
                }
                switch (numRel) {
                    case EQUAL:
                        return compat.term(key, value);
                    case NOT_EQUAL:
                        return compat.boolMustNot(compat.term(key, value));
                    case LESS_THAN:
                        return compat.lt(key, value);
                    case LESS_THAN_EQUAL:
                        return compat.lte(key, value);
                    case GREATER_THAN:
                        return compat.gt(key, value);
                    case GREATER_THAN_EQUAL:
                        return compat.gte(key, value);
                    default:
                        throw new IllegalArgumentException("Unexpected relation: " + numRel);
                }
            } else if (value instanceof Boolean) {
                Cmp numRel = (Cmp) janusgraphPredicate;
                switch (numRel) {
                    case EQUAL:
                        return compat.term(key, value);
                    case NOT_EQUAL:
                        return compat.boolMustNot(compat.term(key, value));
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }
            } else if (value instanceof UUID) {
                if (janusgraphPredicate == Cmp.EQUAL) {
                    return compat.term(key, value);
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return compat.boolMustNot(compat.term(key, value));
                } else {
                    throw new IllegalArgumentException("Only equal or not equal is supported for UUIDs: " + janusgraphPredicate);
                }
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        } else if (condition instanceof Not) {
            return compat.boolMustNot(getFilter(((Not) condition).getChild(),informations));
        } else if (condition instanceof And) {
            final List queries = StreamSupport.stream(condition.getChildren().spliterator(), false)
                .map(c -> getFilter(c,informations)).collect(Collectors.toList());
            return compat.boolMust(queries);
        } else if (condition instanceof Or) {
            final List queries = StreamSupport.stream(condition.getChildren().spliterator(), false)
                .map(c -> getFilter(c,informations)).collect(Collectors.toList());
            return compat.boolShould(queries);
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        ElasticSearchRequest sr = new ElasticSearchRequest();
        final Map<String,Object> esQuery = getFilter(query.getCondition(), informations.get(query.getStore()));
        sr.setQuery(compat.prepareQuery(esQuery));
        if (!query.getOrder().isEmpty()) {
            List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (int i = 0; i < orders.size(); i++) {
                IndexQuery.OrderEntry orderEntry = orders.get(i);
                String order = orderEntry.getOrder().name();
                KeyInformation information = informations.get(query.getStore()).get(orders.get(i).getKey());
                Mapping mapping = Mapping.getMapping(information);
                Class<?> datatype = orderEntry.getDatatype();
                sr.addSort(orders.get(i).getKey(), order.toLowerCase(), convertToEsDataType(datatype, mapping));
            }
        }
        sr.setFrom(0);
        if (query.hasLimit()) sr.setSize(query.getLimit());
        else sr.setSize(maxResultsSize);

        ElasticSearchResponse response;
        try {
            response = client.search(indexName, query.getStore(), compat.createRequestBody(sr));
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }

        log.debug("Executed query [{}] in {} ms", query.getCondition(), response.getTook());
        if (!query.hasLimit() && response.getTotal() >= maxResultsSize)
            log.warn("Query result set truncated to first [{}] elements for query: {}", maxResultsSize, query);
        return response.getResults().stream().map(result -> result.getResult()).collect(Collectors.toList());
    }

    private String convertToEsDataType(Class<?> datatype, Mapping mapping) {
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
            return mapping == Mapping.DEFAULT ? "geo_point" : "geo_shape";
        }

        return null;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        ElasticSearchRequest sr = new ElasticSearchRequest();
        sr.setQuery(compat.queryString(query.getQuery()));

        sr.setFrom(query.getOffset());
        if (query.hasLimit()) sr.setSize(query.getLimit());
        else sr.setSize(maxResultsSize);

        ElasticSearchResponse response;
        try {
            response = client.search(indexName, query.getStore(), compat.createRequestBody(sr));
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }
        log.debug("Executed query [{}] in {} ms", query.getQuery(), response.getTook());
        if (!query.hasLimit() && response.getTotal() >= maxResultsSize)
            log.warn("Query result set truncated to first [{}] elements for query: {}", maxResultsSize, query);
        return response.getResults();
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtil.isString(dataType) &&
                !(mapping==Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType))) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            if (janusgraphPredicate instanceof Cmp) return true;
        } else if (dataType == Geoshape.class) {
            switch(mapping) {
                case DEFAULT:
                    return janusgraphPredicate instanceof Geo && janusgraphPredicate != Geo.CONTAINS;
                case PREFIX_TREE:
                    return janusgraphPredicate instanceof Geo;
            }
        } else if (AttributeUtil.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.CONTAINS_REGEX || janusgraphPredicate == Text.CONTAINS_FUZZY;
                case STRING:
                    return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL || janusgraphPredicate==Text.REGEX || janusgraphPredicate==Text.PREFIX  || janusgraphPredicate == Text.FUZZY;
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
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType== Instant.class || dataType == Boolean.class || dataType == UUID.class) {
            if (mapping==Mapping.DEFAULT) return true;
        } else if (AttributeUtil.isString(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.STRING
                    || mapping==Mapping.TEXT || mapping==Mapping.TEXTSTRING) return true;
        } else if (AttributeUtil.isGeo(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.PREFIX_TREE) return true;
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
        return compat.getIndexFeatures();
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new DefaultTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        try {
            client.close();
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }

    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            client.deleteIndex(indexName);
        } catch (Exception e) {
            throw new PermanentBackendException("Could not delete index " + indexName, e);
        } finally {
            close();
        }
    }

}
