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
import com.google.common.collect.Sets;
import org.elasticsearch.client.RestClientBuilder;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.es.compat.AbstractESCompat;
import org.janusgraph.diskstorage.es.compat.ESCompatUtils;
import org.janusgraph.diskstorage.es.mapping.IndexMapping;
import org.janusgraph.diskstorage.es.rest.util.HttpAuthTypes;
import org.janusgraph.diskstorage.es.script.ESScriptResponse;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.locationtech.spatial4j.shape.Rectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_DOC_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_GEO_COORDS_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_LANG_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_SCRIPT_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_TYPE_KEY;
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

    public static final ConfigOption<String> INTERFACE =
            new ConfigOption<>(ELASTICSEARCH_NS, "interface",
            "Interface for connecting to Elasticsearch. " +
            "TRANSPORT_CLIENT and NODE were previously supported, but now are required to migrate to REST_CLIENT. " +
            "See the JanusGraph upgrade instructions for more details.",
            ConfigOption.Type.MASKABLE, String.class, ElasticSearchSetup.REST_CLIENT.toString(),
            disallowEmpty(String.class));

    public static final ConfigOption<String> HEALTH_REQUEST_TIMEOUT =
            new ConfigOption<>(ELASTICSEARCH_NS, "health-request-timeout",
            "When JanusGraph initializes its ES backend, JanusGraph waits up to this duration for the " +
            "ES cluster health to reach at least yellow status.  " +
            "This string should be formatted as a natural number followed by the lowercase letter " +
            "\"s\", e.g. 3s or 60s.", ConfigOption.Type.MASKABLE, "30s");

    public static final ConfigOption<String> BULK_REFRESH =
            new ConfigOption<>(ELASTICSEARCH_NS, "bulk-refresh",
            "Elasticsearch bulk API refresh setting used to control when changes made by this request are made " +
            "visible to search", ConfigOption.Type.MASKABLE, "false");

    public static final ConfigNamespace ES_CREATE_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "create", "Settings related to index creation");

    public static final ConfigOption<Long> CREATE_SLEEP =
            new ConfigOption<>(ES_CREATE_NS, "sleep",
            "How long to sleep, in milliseconds, between the successful completion of a (blocking) index " +
            "creation request and the first use of that index.  This only applies when creating an index in ES, " +
            "which typically only happens the first time JanusGraph is started on top of ES. If the index JanusGraph is " +
            "configured to use already exists, then this setting has no effect.", ConfigOption.Type.MASKABLE, 200L);

    public static final ConfigNamespace ES_CREATE_EXTRAS_NS =
            new ConfigNamespace(ES_CREATE_NS, "ext", "Overrides for arbitrary settings applied at index creation.\n" +
            		"See [Elasticsearch](../index-backend/elasticsearch.md#index-creation-options), The full list of possible setting is available at " + 
            		"[Elasticsearch index settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-modules-settings).");

    public static final ConfigOption<Boolean> USE_EXTERNAL_MAPPINGS =
            new ConfigOption<>(ES_CREATE_NS, "use-external-mappings",
            "Whether JanusGraph should make use of an external mapping when registering an index.", ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Integer> NUMBER_OF_REPLICAS =
            new ConfigOption<>(ES_CREATE_EXTRAS_NS, "number_of_replicas",
            "The number of replicas each primary shard has", ConfigOption.Type.MASKABLE, 1);

    public static final ConfigOption<Integer> NUMBER_OF_SHARDS =
            new ConfigOption<>(ES_CREATE_EXTRAS_NS, "number_of_shards",
            "The number of primary shards that an index should have." +
            "Default value is 5 on ES 6 and 1 on ES 7", ConfigOption.Type.MASKABLE, Integer.class);

    
    public static final ConfigOption<Boolean> ALLOW_MAPPING_UPDATE =
            new ConfigOption<>(ES_CREATE_NS, "allow-mapping-update",
            "Whether JanusGraph should allow a mapping update when registering an index. " +
            "Only applicable when " + USE_EXTERNAL_MAPPINGS.getName() + " is true.", ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Boolean> USE_ALL_FIELD =
        new ConfigOption<>(ELASTICSEARCH_NS, "use-all-field",
            "Whether JanusGraph should add an \"all\" field mapping. When enabled field mappings will " +
            "include a \"copy_to\" parameter referencing the \"all\" field. This is supported since Elasticsearch 6.x " +
            " and is required when using wildcard fields starting in Elasticsearch 6.x.", ConfigOption.Type.GLOBAL_OFFLINE, true);

    public static final ConfigOption<Integer> ES_SCROLL_KEEP_ALIVE =
            new ConfigOption<>(ELASTICSEARCH_NS, "scroll-keep-alive",
            "How long (in seconds) elasticsearch should keep alive the scroll context.", ConfigOption.Type.GLOBAL_OFFLINE, 60);

    public static final ConfigNamespace ES_INGEST_PIPELINES =
            new ConfigNamespace(ELASTICSEARCH_NS, "ingest-pipeline", "Ingest pipeline applicable to a store of an index.");

    public static final ConfigNamespace SSL_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "ssl", "Elasticsearch SSL configuration");

    public static final ConfigOption<Boolean> SSL_ENABLED =
            new ConfigOption<>(SSL_NS, "enabled",
            "Controls use of the SSL connection to Elasticsearch.", ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Boolean> SSL_DISABLE_HOSTNAME_VERIFICATION =
            new ConfigOption<>(SSL_NS, "disable-hostname-verification",
            "Disables the SSL hostname verification if set to true. Hostname verification is enabled by default.",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Boolean> SSL_ALLOW_SELF_SIGNED_CERTIFICATES =
            new ConfigOption<>(SSL_NS, "allow-self-signed-certificates",
            "Controls the accepting of the self-signed SSL certificates.",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigNamespace SSL_TRUSTSTORE_NS =
            new ConfigNamespace(SSL_NS, "truststore", "Configuration options for SSL Truststore.");

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION =
            new ConfigOption<>(SSL_TRUSTSTORE_NS, "location",
            "Marks the location of the SSL Truststore.", ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            new ConfigOption<>(SSL_TRUSTSTORE_NS, "password",
            "The password to access SSL Truststore.", ConfigOption.Type.LOCAL, "", Objects::nonNull);

    public static final ConfigNamespace SSL_KEYSTORE_NS =
            new ConfigNamespace(SSL_NS, "keystore", "Configuration options for SSL Keystore.");

    public static final ConfigOption<String> SSL_KEYSTORE_LOCATION =
            new ConfigOption<>(SSL_KEYSTORE_NS, "location",
            "Marks the location of the SSL Keystore.", ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            new ConfigOption<>(SSL_KEYSTORE_NS, "storepassword",
            "The password to access SSL Keystore.", ConfigOption.Type.LOCAL, "", Objects::nonNull);

    public static final ConfigOption<String> SSL_KEY_PASSWORD =
            new ConfigOption<>(SSL_KEYSTORE_NS, "keypassword",
            "The password to access the key in the SSL Keystore. If the option is not present, the value of \"storepassword\" is used.",
            ConfigOption.Type.LOCAL, "", Objects::nonNull);

    public static final ConfigNamespace ES_HTTP_NS =
            new ConfigNamespace(ELASTICSEARCH_NS, "http", "Configuration options for HTTP(S) transport.");

    public static final ConfigNamespace ES_HTTP_AUTH_NS =
            new ConfigNamespace(ES_HTTP_NS, "auth", "Configuration options for HTTP(S) authentication.");

    public static final ConfigOption<String> ES_HTTP_AUTH_TYPE =
            new ConfigOption<>(ES_HTTP_AUTH_NS, "type",
            "Authentication type to be used for HTTP(S) access. Available options are `NONE`, `BASIC` and `CUSTOM`.",
            ConfigOption.Type.LOCAL, HttpAuthTypes.NONE.toString());

    public static final ConfigNamespace ES_HTTP_AUTH_BASIC_NS =
            new ConfigNamespace(ES_HTTP_AUTH_NS, "basic", "Configuration options for HTTP(S) Basic authentication.");

    public static final ConfigOption<String> ES_HTTP_AUTH_USERNAME =
            new ConfigOption<>(ES_HTTP_AUTH_BASIC_NS, "username",
            "Username for HTTP(S) authentication.", ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String> ES_HTTP_AUTH_PASSWORD =
            new ConfigOption<>(ES_HTTP_AUTH_BASIC_NS, "password",
            "Password for HTTP(S) authentication.", ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String> ES_HTTP_AUTH_REALM = new ConfigOption<>(ES_HTTP_AUTH_BASIC_NS,
            "realm", "Realm value for HTTP(S) authentication. If empty, any realm is accepted.",
            ConfigOption.Type.LOCAL, "");

    public static final ConfigNamespace ES_HTTP_AUTH_CUSTOM_NS =
            new ConfigNamespace(ES_HTTP_AUTH_NS, "custom", "Configuration options for custom HTTP(S) authenticator.");

    public static final ConfigOption<String> ES_HTTP_AUTHENTICATOR_CLASS = new ConfigOption<>(
            ES_HTTP_AUTH_CUSTOM_NS, "authenticator-class", "Authenticator fully qualified class name.",
            ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String[]> ES_HTTP_AUTHENTICATOR_ARGS = new ConfigOption<>(
            ES_HTTP_AUTH_CUSTOM_NS, "authenticator-args", "Comma-separated custom authenticator constructor arguments.",
            ConfigOption.Type.LOCAL, new String[0]);

    public static final ConfigOption<Boolean> SETUP_MAX_OPEN_SCROLL_CONTEXTS =
        new ConfigOption<>(ELASTICSEARCH_NS, "setup-max-open-scroll-contexts",
            "Whether JanusGraph should setup max_open_scroll_context to maximum value for the cluster or not.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> USE_MAPPING_FOR_ES7 =
        new ConfigOption<>(ELASTICSEARCH_NS, "use-mapping-for-es7",
            "Mapping types are deprecated in ElasticSearch 7 and JanusGraph will not use mapping types by default " +
                "for ElasticSearch 7 but if you want to preserve mapping types, you can setup this parameter to true. " +
                "If you are updating ElasticSearch from 6 to 7 and you don't want to reindex your indexes, you may setup " +
                "this parameter to true but we do recommend to reindex your indexes and don't use this parameter.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Long> CLIENT_KEEP_ALIVE =
        new ConfigOption<>(ELASTICSEARCH_NS, "client-keep-alive",
            "Set a keep-alive timeout (in milliseconds)",
            ConfigOption.Type.GLOBAL_OFFLINE, Long.class);

    public static final ConfigOption<Integer> RETRY_ON_CONFLICT =
        new ConfigOption<>(ELASTICSEARCH_NS, "retry_on_conflict",
            "Specify how many times should the operation be retried when a conflict occurs.", ConfigOption.Type.MASKABLE, 0);

    public static final ConfigOption<Boolean> ENABLE_INDEX_STORE_NAMES_CACHE =
        new ConfigOption<>(ELASTICSEARCH_NS, "enable_index_names_cache",
            "Enables cache for generated index store names. " +
                "It is recommended to always enable index store names cache unless you have more then 50000 indexes " +
                "per index store.", ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Integer> CONNECT_TIMEOUT =
        new ConfigOption<>(ELASTICSEARCH_NS, "connect-timeout",
            "Sets the maximum connection timeout (in milliseconds).", ConfigOption.Type.MASKABLE,
            Integer.class, RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS);

    public static final ConfigOption<Integer> SOCKET_TIMEOUT =
        new ConfigOption<>(ELASTICSEARCH_NS, "socket-timeout",
            "Sets the maximum socket timeout (in milliseconds).", ConfigOption.Type.MASKABLE,
            Integer.class, RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS);

    public static final int HOST_PORT_DEFAULT = 9200;

    /**
     * Default tree_levels used when creating geo_shape mappings.
     */
    public static final int DEFAULT_GEO_MAX_LEVELS = 20;

    /**
     * Default distance_error_pct used when creating geo_shape mappings.
     */
    public static final double DEFAULT_GEO_DIST_ERROR_PCT = 0.025;

    private static final String PARAMETERIZED_DELETION_SCRIPT = parameterizedScriptPrepare("",
            "for (field in params.fields) {",
            "    if (field.cardinality == 'SINGLE') {",
            "        ctx._source.remove(field.name);",
            "    } else if (ctx._source.containsKey(field.name)) {",
            "        def fieldIndex = ctx._source[field.name].indexOf(field.value);",
            "        if (fieldIndex >= 0 && fieldIndex < ctx._source[field.name].size()) {",
            "            ctx._source[field.name].remove(fieldIndex);",
            "        }",
            "    }",
            "}");

    private static final String PARAMETERIZED_ADDITION_SCRIPT = parameterizedScriptPrepare("",
            "for (field in params.fields) {",
            "    if (ctx._source[field.name] == null) {",
            "        ctx._source[field.name] = [];",
            "    }",
            "    if (field.cardinality != 'SET' || ctx._source[field.name].indexOf(field.value) == -1) {",
            "        ctx._source[field.name].add(field.value);",
            "    }",
            "}");

    static final String INDEX_NAME_SEPARATOR = "_";
    private static final String SCRIPT_ID_SEPARATOR = "-";

    private static final String MAX_OPEN_SCROLL_CONTEXT_PARAMETER = "search.max_open_scroll_context";
    private static final Map<String, Object> MAX_RESULT_WINDOW = ImmutableMap.of("index.max_result_window", Integer.MAX_VALUE);

    private static final Parameter[] NULL_PARAMETERS = null;

    private static final String TRACK_TOTAL_HITS_PARAMETER = "track_total_hits";
    private static final Parameter[] TRACK_TOTAL_HITS_DISABLED_PARAMETERS = new Parameter[]{new Parameter<>(TRACK_TOTAL_HITS_PARAMETER, false)};
    private static final Map<String, Object> TRACK_TOTAL_HITS_DISABLED_REQUEST_BODY = ImmutableMap.of(TRACK_TOTAL_HITS_PARAMETER, false);

    private final Function<String, String> generateIndexStoreNameFunction = this::generateIndexStoreName;
    private final Map<String, String> indexStoreNamesCache = new ConcurrentHashMap<>();
    private final boolean indexStoreNameCacheEnabled;

    private final AbstractESCompat compat;
    private final ElasticSearchClient client;
    private final String indexName;
    private final int batchSize;
    private final boolean useExternalMappings;
    private final boolean allowMappingUpdate;
    private final Map<String, Object> indexSetting;
    private final long createSleep;
    private final boolean useAllField;
    private final Map<String, Object> ingestPipelines;
    private final boolean useMappingForES7;
    private final String parameterizedAdditionScriptId;
    private final String parameterizedDeletionScriptId;

    public ElasticSearchIndex(Configuration config) throws BackendException {

        indexName = config.get(INDEX_NAME);
        parameterizedAdditionScriptId = generateScriptId("add");
        parameterizedDeletionScriptId = generateScriptId("del");
        useAllField = config.get(USE_ALL_FIELD);
        useExternalMappings = config.get(USE_EXTERNAL_MAPPINGS);
        allowMappingUpdate = config.get(ALLOW_MAPPING_UPDATE);
        createSleep = config.get(CREATE_SLEEP);
        ingestPipelines = config.getSubset(ES_INGEST_PIPELINES);
        useMappingForES7 = config.get(USE_MAPPING_FOR_ES7);
        indexStoreNameCacheEnabled = config.get(ENABLE_INDEX_STORE_NAMES_CACHE);
        batchSize = config.get(INDEX_MAX_RESULT_SET_SIZE);
        log.debug("Configured ES query nb result by query to {}", batchSize);

        client = interfaceConfiguration(config).getClient();

        checkClusterHealth(config.get(HEALTH_REQUEST_TIMEOUT));

        compat = ESCompatUtils.acquireCompatForVersion(client.getMajorVersion());

        indexSetting = ElasticSearchSetup.getSettingsFromJanusGraphConf(config);

        setupMaxOpenScrollContextsIfNeeded(config);

        setupStoredScripts();
    }

    private void checkClusterHealth(String healthCheck) throws BackendException {
        try {
            client.clusterHealthRequest(healthCheck);
        } catch (final IOException e) {
            throw new PermanentBackendException(e.getMessage(), e);
        }
    }

    private void setupStoredScripts() throws PermanentBackendException {
        setupStoredScriptIfNeeded(parameterizedAdditionScriptId, PARAMETERIZED_ADDITION_SCRIPT);
        setupStoredScriptIfNeeded(parameterizedDeletionScriptId, PARAMETERIZED_DELETION_SCRIPT);
    }

    private void setupStoredScriptIfNeeded(String storedScriptId, String source) throws PermanentBackendException {

        ImmutableMap<String, Object> preparedScript = compat.prepareScript(source).build();

        String lang = (String) ((ImmutableMap<String, Object>) preparedScript.get(ES_SCRIPT_KEY)).get(ES_LANG_KEY);

        try {
            ESScriptResponse esScriptResponse = client.getStoredScript(storedScriptId);

            if(Boolean.FALSE.equals(esScriptResponse.getFound()) || !Objects.equals(lang, esScriptResponse.getScript().getLang()) ||
                !Objects.equals(source, esScriptResponse.getScript().getSource())){
                client.createStoredScript(storedScriptId, preparedScript);
            }

        } catch (final IOException e) {
            throw new PermanentBackendException(e.getMessage(), e);
        }
    }

    private void setupMaxOpenScrollContextsIfNeeded(Configuration config) throws PermanentBackendException {

        if(client.getMajorVersion().getValue() > 6){

            boolean setupMaxOpenScrollContexts;

            if(config.has(SETUP_MAX_OPEN_SCROLL_CONTEXTS)){
                setupMaxOpenScrollContexts = config.get(SETUP_MAX_OPEN_SCROLL_CONTEXTS);
            } else {
                setupMaxOpenScrollContexts = SETUP_MAX_OPEN_SCROLL_CONTEXTS.getDefaultValue();
            }

            if(setupMaxOpenScrollContexts){

                Map<String, Object> settings = ImmutableMap.of("persistent",
                    ImmutableMap.of(MAX_OPEN_SCROLL_CONTEXT_PARAMETER, Integer.MAX_VALUE));

                try {
                    client.updateClusterSettings(settings);
                } catch (final IOException e) {
                    throw new PermanentBackendException(e.getMessage(), e);
                }
            }
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
     * @param index index name
     * @throws IOException if the index status could not be checked or index could not be created
     */
    private void checkForOrCreateIndex(String index) throws IOException {
        Objects.requireNonNull(client);
        Objects.requireNonNull(index);

        // Create index if it does not useExternalMappings and if it does not already exist
        if (!useExternalMappings && !client.indexExists(index)) {
            client.createIndex(index, indexSetting);
            client.updateIndexSettings(index, MAX_RESULT_WINDOW);
            try {
                log.debug("Sleeping {} ms after {} index creation returned from actionGet()", createSleep, index);
                Thread.sleep(createSleep);
            } catch (final InterruptedException e) {
                throw new JanusGraphException("Interrupted while waiting for index to settle in", e);
            }
        }
        Preconditions.checkState(client.indexExists(index), "Could not create index: %s",index);
        client.addAlias(indexName, index);
    }


    /**
     * Configure ElasticSearchIndex's ES client. See{@link org.janusgraph.diskstorage.es.ElasticSearchSetup} for more
     * information.
     *
     * @param config a config passed to ElasticSearchIndex's constructor
     * @return a client object open and ready for use
     */
    private ElasticSearchSetup.Connection interfaceConfiguration(Configuration config) {
        final ElasticSearchSetup clientMode = ConfigOption.getEnumValue(config.get(INTERFACE), ElasticSearchSetup.class);

        try {
            return clientMode.connect(config);
        } catch (final IOException e) {
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

    private String generateScriptId(String uniqueScriptSuffix){
        return indexName + SCRIPT_ID_SEPARATOR + uniqueScriptSuffix;
    }

    private String generateIndexStoreName(String store){
        return indexName + INDEX_NAME_SEPARATOR + store.toLowerCase();
    }

    private String getIndexStoreName(String store) {

        if(indexStoreNameCacheEnabled){
            return indexStoreNamesCache.computeIfAbsent(store, generateIndexStoreNameFunction);
        }

        return generateIndexStoreName(store);
    }

    @Override
    public void register(String store, String key, KeyInformation information,
                         BaseTransaction tx) throws BackendException {
        final Class<?> dataType = information.getDataType();
        final Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map==Mapping.DEFAULT || AttributeUtils.isString(dataType) ||
                (map==Mapping.PREFIX_TREE && AttributeUtils.isGeo(dataType)),
                "Specified illegal mapping [%s] for data type [%s]",map,dataType);
        final String indexStoreName = getIndexStoreName(store);
        if (useExternalMappings) {
            try {
                //We check if the externalMapping have the property 'key'
                final IndexMapping mappings = client.getMapping(indexStoreName, store);
                if (mappings == null || (!mappings.isDynamic() && !mappings.getProperties().containsKey(key))) {
                    //Error if it is not dynamic and have not the property 'key'
                    throw new PermanentBackendException("The external mapping for index '"+ indexStoreName + "' and type '" + store + "' do not have property '" + key + "'");
                } else if (allowMappingUpdate && mappings.isDynamic()) {
                    //If it is dynamic, we push the unknown property 'key'
                    this.pushMapping(store, key, information);
                }
            } catch (final IOException e) {
                throw new PermanentBackendException(e);
            }
        } else {
            try {
                checkForOrCreateIndex(indexStoreName);
            } catch (final IOException e) {
                throw new PermanentBackendException(e);
            }
            this.pushMapping(store, key, information);
        }
    }

    /**
     * Push mapping to ElasticSearch
     * @param store the type in the index
     * @param key the name of the property in the index
     * @param information information of the key
     */
    private void pushMapping(String store, String key,
                             KeyInformation information) throws AssertionError, BackendException {
        final Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        final Map<String,Object> properties = new HashMap<>();
        if (AttributeUtils.isString(dataType)) {
            if (map==Mapping.DEFAULT) map=Mapping.TEXT;
            log.debug("Registering string type for {} with mapping {}", key, map);
            final String stringAnalyzer
                    = ParameterType.STRING_ANALYZER.findParameter(information.getParameters(), null);
            final String textAnalyzer = ParameterType.TEXT_ANALYZER.findParameter(information.getParameters(), null);
            // use keyword type for string mappings unless custom string analyzer is provided
            final Map<String,Object> stringMapping
                    = stringAnalyzer == null ? compat.createKeywordMapping() : compat.createTextMapping(stringAnalyzer);
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
                    final int maxLevels = ParameterType.INDEX_GEO_MAX_LEVELS.findParameter(information.getParameters(),
                            DEFAULT_GEO_MAX_LEVELS);
                    final double distErrorPct
                            = ParameterType.INDEX_GEO_DIST_ERROR_PCT.findParameter(information.getParameters(),
                                                                                   DEFAULT_GEO_DIST_ERROR_PCT);
                    log.debug("Registering geo_shape type for {} with tree_levels={} and distance_error_pct={}", key,
                            maxLevels, distErrorPct);
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

        if (useAllField) {
            // add custom all field mapping if it doesn't exist
            properties.put(ElasticSearchConstants.CUSTOM_ALL_FIELD, compat.createTextMapping(null));

            // add copy_to for custom all field mapping
            if (properties.containsKey(key) && dataType != Geoshape.class) {
                final Map<String, Object> mapping = new HashMap<>(((Map<String, Object>) properties.get(key)));
                mapping.put("copy_to", ElasticSearchConstants.CUSTOM_ALL_FIELD);
                properties.put(key, mapping);
            }
        }

        final List<Parameter> customParameters = ParameterType.getCustomParameters(information.getParameters());

        if (properties.containsKey(key) && !customParameters.isEmpty()) {
            final Map<String, Object> mapping = new HashMap<>(((Map<String, Object>) properties.get(key)));
            customParameters.forEach(p -> mapping.put(p.key(), p.value()));
            properties.put(key, mapping);
        }

        final Map<String,Object> mapping = ImmutableMap.of("properties", properties);

        try {
            client.createMapping(getIndexStoreName(store), store, mapping);
        } catch (final Exception e) {
            throw convert(e);
        }
    }

    private static Mapping getStringMapping(KeyInformation information) {
        assert AttributeUtils.isString(information.getDataType());
        Mapping map = Mapping.getMapping(information);
        if (map==Mapping.DEFAULT) map = Mapping.TEXT;
        return map;
    }

    private static boolean hasDualStringMapping(KeyInformation information) {
        return AttributeUtils.isString(information.getDataType()) && getStringMapping(information)==Mapping.TEXTSTRING;
    }

    public Map<String, Object> getNewDocument(final List<IndexEntry> additions,
                                              KeyInformation.StoreRetriever information) throws BackendException {
        // JSON writes duplicate fields one after another, which forces us
        // at this stage to make de-duplication on the IndexEntry list. We don't want to pay the
        // price map storage on the Mutation level because none of other backends need that.

        final Multimap<String, IndexEntry> unique = LinkedListMultimap.create();
        for (final IndexEntry e : additions) {
            unique.put(e.field, e);
        }

        final Map<String, Object> doc = new HashMap<>();
        for (final Map.Entry<String, Collection<IndexEntry>> add : unique.asMap().entrySet()) {
            final KeyInformation keyInformation = information.get(add.getKey());
            final Object value;
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    value = convertToEsType(Iterators.getLast(add.getValue().iterator()).value,
                            Mapping.getMapping(keyInformation));
                    break;
                case SET:
                case LIST:
                    value = add.getValue().stream()
                        .map(v -> convertToEsType(v.value, Mapping.getMapping(keyInformation)))
                        .filter(v -> {
                            Preconditions.checkArgument(!(v instanceof byte[]),
                                "Collections not supported for %s", add.getKey());
                            return true;
                        }).toArray();
                    break;
                default:
                    value = null;
                    break;
            }

            doc.put(add.getKey(), value);
            if (hasDualStringMapping(information.get(add.getKey())) && keyInformation.getDataType() == String.class) {
                doc.put(getDualMappingName(add.getKey()), value);
            }


        }

        return doc;
    }

    private static Object convertToEsType(Object value, Mapping mapping) {
        if (value instanceof Number) {
            if (AttributeUtils.isWholeNumber((Number) value)) {
                return ((Number) value).longValue();
            } else { //double or float
                return ((Number) value).doubleValue();
            }
        } else if (AttributeUtils.isString(value)) {
            return value;
        } else if (value instanceof Geoshape) {
            return convertGeoshape((Geoshape) value, mapping);
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
    private static Object convertGeoshape(Geoshape geoshape, Mapping mapping) {
        if (geoshape.getType() == Geoshape.Type.POINT && Mapping.PREFIX_TREE != mapping) {
            final Geoshape.Point p = geoshape.getPoint();
            return new double[]{p.getLongitude(), p.getLatitude()};
        } else if (geoshape.getType() == Geoshape.Type.BOX) {
            final Rectangle box = geoshape.getShape().getBoundingBox();
            final Map<String,Object> map = new HashMap<>();
            map.put("type", "envelope");
            map.put("coordinates", new double[][] {{box.getMinX(),box.getMaxY()},{box.getMaxX(),box.getMinY()}});
            return map;
        } else if (geoshape.getType() == Geoshape.Type.CIRCLE) {
            try {
                final Map<String,Object> map = geoshape.toMap();
                map.put("radius", map.get("radius") + ((Map<String, String>) map.remove("properties")).get("radius_units"));
                return map;
            } catch (final IOException e) {
                throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
            }
        } else {
            try {
                return geoshape.toMap();
            } catch (final IOException e) {
                throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
            }
        }
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information,
                       BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
        try {
            for (final Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                final List<ElasticSearchMutation> requestByStore = new ArrayList<>();
                final String storeName = stores.getKey();
                final String indexStoreName = getIndexStoreName(storeName);
                for (final Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    final String documentId = entry.getKey();
                    final IndexMutation mutation = entry.getValue();
                    assert mutation.isConsolidated();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());
                    //Deletions first
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            log.trace("Deleting entire document {}", documentId);
                            requestByStore.add(ElasticSearchMutation.createDeleteRequest(indexStoreName, storeName,
                                    documentId));
                        } else {
                            List<Map<String, Object>> params = getParameters(information.get(storeName),
                                mutation.getDeletions(), true);
                            Map doc = compat.prepareStoredScript(parameterizedDeletionScriptId, params).build();
                            log.trace("Deletion script {} with params {}", PARAMETERIZED_DELETION_SCRIPT, params);
                            requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, storeName,
                                documentId, doc));
                        }
                    }
                    if (mutation.hasAdditions()) {
                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", documentId);
                            final Map<String, Object> source = getNewDocument(mutation.getAdditions(),
                                    information.get(storeName));
                            requestByStore.add(ElasticSearchMutation.createIndexRequest(indexStoreName, storeName,
                                    documentId, source));
                        } else {
                            final Map upsert;
                            if (!mutation.hasDeletions()) {
                                upsert = getNewDocument(mutation.getAdditions(), information.get(storeName));
                            } else {
                                upsert = null;
                            }

                            List<Map<String, Object>> params = getParameters(information.get(storeName),
                                    mutation.getAdditions(), false, Cardinality.SINGLE);
                            if (!params.isEmpty()) {
                                ImmutableMap.Builder builder = compat.prepareStoredScript(parameterizedAdditionScriptId, params);
                                requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, storeName,
                                        documentId, builder, upsert));
                                log.trace("Adding script {} with params {}", PARAMETERIZED_ADDITION_SCRIPT, params);
                            }

                            final Map<String, Object> doc = getAdditionDoc(information, storeName, mutation);
                            if (!doc.isEmpty()) {
                                final ImmutableMap.Builder builder = ImmutableMap.builder().put(ES_DOC_KEY, doc);
                                requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, storeName,
                                        documentId, builder, upsert));
                                log.trace("Adding update {}", doc);
                            }
                        }
                    }
                }
                if (!requestByStore.isEmpty() && ingestPipelines.containsKey(storeName)) {
                    client.bulkRequest(requestByStore, String.valueOf(ingestPipelines.get(storeName)));
                } else if (!requestByStore.isEmpty()) {
                    requests.addAll(requestByStore);
                }
            }
            if (!requests.isEmpty()) {
                client.bulkRequest(requests, null);
            }
        } catch (final Exception e) {
            log.error("Failed to execute bulk Elasticsearch mutation", e);
            throw convert(e);
        }
    }

    private List<Map<String, Object>> getParameters(KeyInformation.StoreRetriever storeRetriever,
                                                    List<IndexEntry> entries,
                                                    boolean deletion,
                                                    Cardinality... cardinalitiesToSkip) {
        Set<Cardinality> cardinalityToSkipSet = Sets.newHashSet(cardinalitiesToSkip);
        List<Map<String, Object>> result = new ArrayList<>();
        for (IndexEntry entry : entries) {
            KeyInformation info = storeRetriever.get(entry.field);
            if (cardinalityToSkipSet.contains(info.getCardinality())) {
                continue;
            }
            Object jsValue = deletion && info.getCardinality() == Cardinality.SINGLE ?
                "" : convertToEsType(entry.value, Mapping.getMapping(info));
            result.add(ImmutableMap.of("name", entry.field,
                    "value", jsValue,
                    "cardinality", info.getCardinality().name()));
            if (hasDualStringMapping(info)) {
                result.add(ImmutableMap.of("name", getDualMappingName(entry.field),
                        "value", jsValue,
                        "cardinality", info.getCardinality().name()));
            }
        }
        return result;
    }

    private Map<String,Object> getAdditionDoc(KeyInformation.IndexRetriever information,
                                              String store, IndexMutation mutation) throws PermanentBackendException {
        final Map<String,Object> doc = new HashMap<>();
        for (final IndexEntry e : mutation.getAdditions()) {
            final KeyInformation keyInformation = information.get(store).get(e.field);
            if (keyInformation.getCardinality() == Cardinality.SINGLE) {
                doc.put(e.field, convertToEsType(e.value, Mapping.getMapping(keyInformation)));
                if (hasDualStringMapping(keyInformation)) {
                    doc.put(getDualMappingName(e.field), convertToEsType(e.value, Mapping.getMapping(keyInformation)));
                }
            }
        }

        return doc;
    }

    @Override
    public void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information,
                        BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
        try {
            for (final Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final List<ElasticSearchMutation> requestByStore = new ArrayList<>();
                final String store = stores.getKey();
                final String indexStoreName = getIndexStoreName(store);
                for (final Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();
                    if (content == null || content.size() == 0) {
                        // delete
                        if (log.isTraceEnabled())
                            log.trace("Deleting entire document {}", docID);

                        requestByStore.add(ElasticSearchMutation.createDeleteRequest(indexStoreName, store, docID));
                    } else {
                        // Add
                        if (log.isTraceEnabled())
                            log.trace("Adding entire document {}", docID);
                        final Map<String, Object> source = getNewDocument(content, information.get(store));
                        requestByStore.add(ElasticSearchMutation.createIndexRequest(indexStoreName, store, docID,
                                source));
                    }
                }
                if (!requestByStore.isEmpty() && ingestPipelines.containsKey(store)) {
                    client.bulkRequest(requestByStore, String.valueOf(ingestPipelines.get(store)));
                } else if (!requestByStore.isEmpty()) {
                    requests.addAll(requestByStore);
                }
            }
            if (!requests.isEmpty())
                client.bulkRequest(requests, null);
        } catch (final Exception e) {
            throw convert(e);
        }
    }

    private Map<String, Object> getRelationFromCmp(final Cmp cmp, String key, final Object value) {
        switch (cmp) {
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
                throw new IllegalArgumentException("Unexpected relation: " + cmp);
        }
    }

    public Map<String,Object> getFilter(Condition<?> condition, KeyInformation.StoreRetriever information) {
        if (condition instanceof PredicateCondition) {
            final PredicateCondition<String, ?> atom = (PredicateCondition) condition;
            Object value = atom.getValue();
            final String key = atom.getKey();
            final JanusGraphPredicate predicate = atom.getPredicate();
            if (value == null && predicate == Cmp.NOT_EQUAL) {
                return compat.exists(key);
            } else if (value instanceof Number) {
                Preconditions.checkArgument(predicate instanceof Cmp,
                        "Relation not supported on numeric types: %s", predicate);
                return getRelationFromCmp((Cmp) predicate, key, value);
            } else if (value instanceof String) {

                final Mapping mapping = getStringMapping(information.get(key));
                final String fieldName;
                if (mapping==Mapping.TEXT && !(Text.HAS_CONTAINS.contains(predicate) || predicate instanceof Cmp))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS and Compare queries and not: " + predicate);
                if (mapping==Mapping.STRING && Text.HAS_CONTAINS.contains(predicate))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + predicate);
                if (mapping==Mapping.TEXTSTRING && !(Text.HAS_CONTAINS.contains(predicate) || (predicate instanceof Cmp && predicate != Cmp.EQUAL))) {
                    fieldName = getDualMappingName(key);
                } else {
                    fieldName = key;
                }

                if (predicate == Text.CONTAINS || predicate == Cmp.EQUAL) {
                    return compat.match(fieldName, value);
                } else if (predicate == Text.NOT_CONTAINS) {
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.match(fieldName, value))));
                } else if (predicate == Text.CONTAINS_PHRASE) {
                    return compat.matchPhrase(fieldName, value);
                } else if (predicate == Text.NOT_CONTAINS_PHRASE) {
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.matchPhrase(fieldName, value))));
                } else if (predicate == Text.CONTAINS_PREFIX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(information.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.prefix(fieldName, value);
                } else if (predicate == Text.NOT_CONTAINS_PREFIX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(information.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.prefix(fieldName, value))));
                } else if (predicate == Text.CONTAINS_REGEX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(information.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.regexp(fieldName, value);
                } else if (predicate == Text.NOT_CONTAINS_REGEX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(information.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.regexp(fieldName, value))));
                } else if (predicate == Text.PREFIX) {
                    return compat.prefix(fieldName, value);
                } else if (predicate == Text.NOT_PREFIX) {
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.prefix(fieldName, value))));
                } else if (predicate == Text.REGEX) {
                    return compat.regexp(fieldName, value);
                } else if (predicate == Text.NOT_REGEX) {
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.regexp(fieldName, value))));
                } else if (predicate == Cmp.NOT_EQUAL) {
                    return compat.boolMustNot(compat.match(fieldName, value));
                } else if (predicate == Text.FUZZY || predicate == Text.CONTAINS_FUZZY) {
                    return compat.fuzzyMatch(fieldName, value);
                } else if (predicate == Text.NOT_FUZZY || predicate == Text.NOT_CONTAINS_FUZZY) {
                    return compat.boolMust(ImmutableList.of(compat.exists(fieldName),
                        compat.boolMustNot(compat.fuzzyMatch(fieldName, value))));
                } else if (predicate == Cmp.LESS_THAN) {
                    return compat.lt(fieldName, value);
                } else if (predicate == Cmp.LESS_THAN_EQUAL) {
                    return compat.lte(fieldName, value);
                } else if (predicate == Cmp.GREATER_THAN) {
                    return compat.gt(fieldName, value);
                } else if (predicate == Cmp.GREATER_THAN_EQUAL) {
                    return compat.gte(fieldName, value);
                } else
                    throw new IllegalArgumentException("Predicate is not supported for string value: " + predicate);
            } else if (value instanceof Geoshape && Mapping.getMapping(information.get(key)) == Mapping.DEFAULT) {
                // geopoint
                final Geoshape shape = (Geoshape) value;
                Preconditions.checkArgument(predicate instanceof Geo && predicate != Geo.CONTAINS,
                        "Relation not supported on geopoint types: %s", predicate);

                final Map<String,Object> query;
                switch (shape.getType()) {
                    case CIRCLE:
                        final Geoshape.Point center = shape.getPoint();
                        query = compat.geoDistance(key, center.getLatitude(), center.getLongitude(), shape.getRadius());
                        break;
                    case BOX:
                        final Geoshape.Point southwest = shape.getPoint(0);
                        final Geoshape.Point northeast = shape.getPoint(1);
                        query = compat.geoBoundingBox(key, southwest.getLatitude(), southwest.getLongitude(),
                            northeast.getLatitude(), northeast.getLongitude());
                        break;
                    case POLYGON:
                        final List<List<Double>> points = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(),
                                shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        query = compat.geoPolygon(key, points);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported or invalid search shape type for geopoint: "
                            + shape.getType());
                }

                return predicate == Geo.DISJOINT ?  compat.boolMustNot(query) : query;
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(predicate instanceof Geo,
                        "Relation not supported on geoshape types: %s", predicate);
                final Geoshape shape = (Geoshape) value;
                final Map<String,Object> geo;
                switch (shape.getType()) {
                    case CIRCLE:
                        final Geoshape.Point center = shape.getPoint();
                        geo = ImmutableMap.of(ES_TYPE_KEY, "circle",
                            ES_GEO_COORDS_KEY, ImmutableList.of(center.getLongitude(), center.getLatitude()),
                            "radius", shape.getRadius() + "km");
                        break;
                    case BOX:
                        final Geoshape.Point southwest = shape.getPoint(0);
                        final Geoshape.Point northeast = shape.getPoint(1);
                        geo = ImmutableMap.of(ES_TYPE_KEY, "envelope",
                            ES_GEO_COORDS_KEY,
                            ImmutableList.of(
                                ImmutableList.of(southwest.getLongitude(), northeast.getLatitude()),
                                ImmutableList.of(northeast.getLongitude(), southwest.getLatitude())));
                        break;
                    case LINE:
                        final List lineCoords = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(),
                                    shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        geo = ImmutableMap.of(ES_TYPE_KEY, "linestring", ES_GEO_COORDS_KEY, lineCoords);
                        break;
                    case POLYGON:
                        final List polyCoords = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(),
                                    shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        geo = ImmutableMap.of(ES_TYPE_KEY, "polygon", ES_GEO_COORDS_KEY,
                                ImmutableList.of(polyCoords));
                        break;
                    case POINT:
                        geo = ImmutableMap.of(ES_TYPE_KEY, "point",
                            ES_GEO_COORDS_KEY, ImmutableList.of(shape.getPoint().getLongitude(),
                                    shape.getPoint().getLatitude()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported or invalid search shape type: "
                                + shape.getType());
                }

                return compat.geoShape(key, geo, (Geo) predicate);
            } else if (value instanceof Date || value instanceof Instant) {
                Preconditions.checkArgument(predicate instanceof Cmp,
                        "Relation not supported on date types: %s", predicate);

                if (value instanceof Instant) {
                    value = Date.from((Instant) value);
                }
                return getRelationFromCmp((Cmp) predicate, key, value);
            } else if (value instanceof Boolean) {
                final Cmp numRel = (Cmp) predicate;
                switch (numRel) {
                    case EQUAL:
                        return compat.term(key, value);
                    case NOT_EQUAL:
                        return compat.boolMustNot(compat.term(key, value));
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }
            } else if (value instanceof UUID) {
                if (predicate == Cmp.EQUAL) {
                    return compat.term(key, value);
                } else if (predicate == Cmp.NOT_EQUAL) {
                    return compat.boolMustNot(compat.term(key, value));
                } else {
                    throw new IllegalArgumentException("Only equal or not equal is supported for UUIDs: "
                            + predicate);
                }
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        } else if (condition instanceof Not) {
            return compat.boolMustNot(getFilter(((Not) condition).getChild(),information));
        } else if (condition instanceof And) {
            final List queries = StreamSupport.stream(condition.getChildren().spliterator(), false)
                .map(c -> getFilter(c,information)).collect(Collectors.toList());
            return compat.boolMust(queries);
        } else if (condition instanceof Or) {
            final List queries = StreamSupport.stream(condition.getChildren().spliterator(), false)
                .map(c -> getFilter(c,information)).collect(Collectors.toList());
            return compat.boolShould(queries);
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever informations,
                                BaseTransaction tx) throws BackendException {
        final ElasticSearchRequest sr = new ElasticSearchRequest();
        final Map<String,Object> esQuery = getFilter(query.getCondition(), informations.get(query.getStore()));
        sr.setQuery(compat.prepareQuery(esQuery));
        if (!query.getOrder().isEmpty()) {
            addOrderToQuery(informations, sr, query.getOrder(), query.getStore());
        }
        sr.setFrom(0);
        if (query.hasLimit()) {
            sr.setSize(Math.min(query.getLimit(), batchSize));
        } else {
            sr.setSize(batchSize);
        }

        sr.setDisableSourceRetrieval(true);

        ElasticSearchResponse response;
        try {
            final String indexStoreName = getIndexStoreName(query.getStore());
            final boolean useScroll = sr.getSize() >= batchSize;
            response = client.search(indexStoreName,
                compat.createRequestBody(sr, useScroll? NULL_PARAMETERS : TRACK_TOTAL_HITS_DISABLED_PARAMETERS),
                useScroll);
            log.debug("First Executed query [{}] in {} ms", query.getCondition(), response.getTook());
            final Iterator<RawQuery.Result<String>> resultIterator = getResultsIterator(useScroll, response, sr.getSize());
            final Stream<RawQuery.Result<String>> toReturn
                    = StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false);
            return (query.hasLimit() ? toReturn.limit(query.getLimit()) : toReturn).map(RawQuery.Result::getResult);
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    private Iterator<RawQuery.Result<String>> getResultsIterator(boolean useScroll, ElasticSearchResponse response, int windowSize){
        return (useScroll)? new ElasticSearchScroll(client, response, windowSize) : response.getResults().iterator();
    }

    private String convertToEsDataType(Class<?> dataType, Mapping mapping) {
        if(String.class.isAssignableFrom(dataType)) {
            return "string";
        }
        else if (Integer.class.isAssignableFrom(dataType)) {
            return "integer";
        }
        else if (Long.class.isAssignableFrom(dataType)) {
            return "long";
        }
        else if (Float.class.isAssignableFrom(dataType)) {
            return "float";
        }
        else if (Double.class.isAssignableFrom(dataType)) {
            return "double";
        }
        else if (Boolean.class.isAssignableFrom(dataType)) {
            return "boolean";
        }
        else if (Date.class.isAssignableFrom(dataType)) {
            return "date";
        }
        else if (Instant.class.isAssignableFrom(dataType)) {
            return "date";
        }
        else if (Geoshape.class.isAssignableFrom(dataType)) {
            return mapping == Mapping.DEFAULT ? "geo_point" : "geo_shape";
        }

        return null;
    }

    private ElasticSearchResponse runCommonQuery(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx, int size,
                                                 boolean useScroll) throws BackendException{
        final ElasticSearchRequest sr = new ElasticSearchRequest();
        sr.setQuery(compat.queryString(query.getQuery()));
        if (!query.getOrders().isEmpty()) {
            addOrderToQuery(informations, sr, query.getOrders(), query.getStore());
        }
        sr.setFrom(0);
        sr.setSize(size);
        sr.setDisableSourceRetrieval(true);
        try {
            Map<String, Object> requestBody = compat.createRequestBody(sr, query.getParameters());
            if(!useScroll) {
                if (requestBody == null) {
                    requestBody = TRACK_TOTAL_HITS_DISABLED_REQUEST_BODY;
                } else {
                    requestBody.put(TRACK_TOTAL_HITS_PARAMETER, false);
                }
            }
            return client.search(
                getIndexStoreName(query.getStore()),
                requestBody,
                useScroll);
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    private long runCountQuery(RawQuery query) throws BackendException{
        try {
            return client.countTotal(
                getIndexStoreName(query.getStore()),
                compat.createRequestBody(compat.queryString(query.getQuery()), query.getParameters()));
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    private void addOrderToQuery(KeyInformation.IndexRetriever informations, ElasticSearchRequest sr, final List<IndexQuery.OrderEntry> orders,
                                 String store) {
        for (final IndexQuery.OrderEntry orderEntry : orders) {
            final String order = orderEntry.getOrder().name();
            final KeyInformation information = informations.get(store).get(orderEntry.getKey());
            final Mapping mapping = Mapping.getMapping(information);
            final Class<?> datatype = orderEntry.getDatatype();
            final String key = hasDualStringMapping(information) ? getDualMappingName(orderEntry.getKey()) : orderEntry.getKey();
            sr.addSort(key, order.toLowerCase(), convertToEsDataType(datatype, mapping));
        }
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information,
                                                 BaseTransaction tx) throws BackendException {
        final int size = query.hasLimit() ? Math.min(query.getLimit() + query.getOffset(), batchSize) : batchSize;
        final boolean useScroll = size >= batchSize;
        final ElasticSearchResponse response = runCommonQuery(query, information, tx, size, useScroll);
        log.debug("First Executed query [{}] in {} ms", query.getQuery(), response.getTook());
        final Iterator<RawQuery.Result<String>> resultIterator = getResultsIterator(useScroll, response, size);
        final Stream<RawQuery.Result<String>> toReturn
                = StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED),
                false).skip(query.getOffset());
        return query.hasLimit() ? toReturn.limit(query.getLimit()) : toReturn;
    }

    @Override
    public Long queryCount(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        final ElasticSearchRequest sr = new ElasticSearchRequest();
        final Map<String,Object> esQuery = getFilter(query.getCondition(), information.get(query.getStore()));
        sr.setQuery(compat.prepareQuery(esQuery));
        try {
            return client.countTotal(
                getIndexStoreName(query.getStore()),
                compat.createRequestBody(sr, null));
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information,
                       BaseTransaction tx) throws BackendException {
        long startTime = System.currentTimeMillis();
        long count = runCountQuery(query);
        if(log.isDebugEnabled()){
            log.debug("Executed count query [{}] in {} ms", query.getQuery(), System.currentTimeMillis() - startTime);
        }
        return count;
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtils.isString(dataType) &&
                !(mapping==Mapping.PREFIX_TREE && AttributeUtils.isGeo(dataType))) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Geoshape.class) {
            switch(mapping) {
                case DEFAULT:
                    return janusgraphPredicate instanceof Geo && janusgraphPredicate != Geo.CONTAINS;
                case PREFIX_TREE:
                    return janusgraphPredicate instanceof Geo;
            }
        } else if (AttributeUtils.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.NOT_CONTAINS
                            || janusgraphPredicate == Text.CONTAINS_FUZZY || janusgraphPredicate == Text.NOT_CONTAINS_FUZZY
                            || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.NOT_CONTAINS_PREFIX
                            || janusgraphPredicate == Text.CONTAINS_REGEX || janusgraphPredicate == Text.NOT_CONTAINS_REGEX
                            || janusgraphPredicate == Text.CONTAINS_PHRASE || janusgraphPredicate == Text.NOT_CONTAINS_PHRASE;
                case STRING:
                    return janusgraphPredicate instanceof Cmp
                            || janusgraphPredicate==Text.REGEX || janusgraphPredicate==Text.NOT_REGEX
                            || janusgraphPredicate==Text.PREFIX || janusgraphPredicate==Text.NOT_PREFIX
                            || janusgraphPredicate == Text.FUZZY || janusgraphPredicate == Text.NOT_FUZZY;
                case TEXTSTRING:
                    return janusgraphPredicate instanceof Text || janusgraphPredicate instanceof Cmp;
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Boolean.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        } else if (dataType == UUID.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL;
        }
        return false;
    }


    @Override
    public boolean supports(KeyInformation information) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType== Instant.class
                || dataType == Boolean.class || dataType == UUID.class) {
            return mapping == Mapping.DEFAULT;
        } else if (AttributeUtils.isString(dataType)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.STRING
                || mapping == Mapping.TEXT || mapping == Mapping.TEXTSTRING;
        } else if (AttributeUtils.isGeo(dataType)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.PREFIX_TREE;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        IndexProvider.checkKeyValidity(key);
        return key.replace(' ', IndexProvider.REPLACEMENT_CHAR);
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
        } catch (final IOException e) {
            throw new PermanentBackendException(e);
        }

    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            client.deleteIndex(indexName);
        } catch (final Exception e) {
            throw new PermanentBackendException("Could not delete index " + indexName, e);
        } finally {
            close();
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return client.indexExists(indexName);
        } catch (final IOException e) {
            throw new PermanentBackendException("Could not check if index " + indexName + " exists", e);
        }
    }

    ElasticMajorVersion getVersion() {
        return client.getMajorVersion();
    }

    boolean isUseMappingForES7(){
        return useMappingForES7;
    }

    private static String parameterizedScriptPrepare(String ... lines){
        return Arrays.stream(lines).map(String::trim).collect(Collectors.joining(""));
    }
}
