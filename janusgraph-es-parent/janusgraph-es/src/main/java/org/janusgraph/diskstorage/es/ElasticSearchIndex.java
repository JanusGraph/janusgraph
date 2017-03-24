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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.Update;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.OpenIndex;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.mapping.PutMapping;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

    public static final ConfigOption<Long> DISCOVERY_FREQUENCY = new ConfigOption<Long>(
        ELASTICSEARCH_NS,
        "discovery-frequency",
        "How long to wait between polling for node discovery",
        ConfigOption.Type.MASKABLE,
        10l
    );

    public static final ConfigOption<String> TTL_INTERVAL =
            new ConfigOption<String>(ELASTICSEARCH_NS, "ttl-interval",
            "The period of time between runs of ES's bulit-in expired document deleter.  " +
            "This string will become the value of ES's indices.ttl.interval setting and should " +
            "be formatted accordingly, e.g. 5s or 60s.", ConfigOption.Type.MASKABLE, "5s");

    public static final ConfigOption<String> CLUSTER_NAME =
            new ConfigOption<String>(ELASTICSEARCH_NS, "cluster-name",
            "The name of the Elasticsearch cluster.  This should match the \"cluster.name\" setting " +
            "in the Elasticsearch nodes' configuration.", ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");

    public static final ConfigOption<Boolean> CLIENT_SNIFF =
            new ConfigOption<Boolean>(ELASTICSEARCH_NS, "sniff",
            "Whether to enable cluster sniffing." +
            "Enabling this option makes the client attempt to discover other cluster nodes " +
            "besides those in the initial host list provided at startup.", ConfigOption.Type.MASKABLE, true);

    private static final ConfigOption<String> ES_TRANSPORT_SCHEME = new ConfigOption<String>(
        ELASTICSEARCH_NS,
        "transport-scheme",
        "What scheme (http or https) to use for communications with Elasticsearch",
        ConfigOption.Type.MASKABLE,
        "https");

    public static final ConfigOption<Integer> TIMEOUT = new ConfigOption<Integer>(
        ELASTICSEARCH_NS,
        "timeout",
        "How long to wait (in ms) for normal operations to complete",
        ConfigOption.Type.MASKABLE,
        30000
    );

    public static final ConfigOption<Integer> METADATA_TIMEOUT = new ConfigOption<Integer>(
        ELASTICSEARCH_NS,
        "metadata-timeout",
        "How long to wait (in ms) for metadata operations to complete",
        ConfigOption.Type.MASKABLE,
        30000
    );

    public static final ConfigOption<Integer> SHARD_COUNT = new ConfigOption<Integer>(
        ELASTICSEARCH_NS,
        "shard-count",
        "How many primary shards to create per Elasticsearch index",
        ConfigOption.Type.MASKABLE,
        5
    );

    public static final ConfigOption<Integer> REPLICA_COUNT = new ConfigOption<Integer>(
        ELASTICSEARCH_NS,
        "replica-count",
        "How many replicas to create per Elasticsearch index",
        ConfigOption.Type.MASKABLE,
        2
    );

    private static final String deletionScript = ""
        + "singles.each { "
        + "  ctx._source.remove((String)it); "
        + "}; "
        + "lists.each { "
        + "  def index = ctx._source[(String)it[0]].indexOf(it[1]); "
        + "  ctx._source[(String)it[0]].remove(index); "
        + "};";

    private static final String additionScript = ""
        + "singles.each { "
        + "  ctx._source[it[0]] = it[1]; "
        + "}; "
        + "lists.each { "
        + "  if (ctx._source[it[0]] == null) { "
        + "    ctx._source[it[0]] = []; "
        + "  }; "
        + "  ctx._source[it[0]].add(it[1]); "
        + "};";


    private static final IndexFeatures ES_FEATURES = new IndexFeatures.Builder().supportsDocumentTTL()
            .setDefaultStringMapping(Mapping.TEXT).supportedStringMappings(Mapping.TEXT, Mapping.TEXTSTRING, Mapping.STRING).setWildcardField("_all").supportsCardinality(Cardinality.SINGLE).supportsCardinality(Cardinality.LIST).supportsCardinality(Cardinality.SET).supportsNanoseconds().build();


    public static final int HOST_PORT_DEFAULT = 9200;

    private final String indexName;
    private final int maxResultsSize;
    private final Configuration configuration;
    private final JestClient client;
    private final String metadataTimeout;
    private final String timeout;

    public ElasticSearchIndex(Configuration config) {
        this.configuration = config;
        this.metadataTimeout = config.get(METADATA_TIMEOUT).toString() + "ms";
        this.timeout = config.get(TIMEOUT).toString() + "ms";
        this.indexName = config.get(INDEX_NAME);
        this.maxResultsSize = config.get(INDEX_MAX_RESULT_SET_SIZE);
        this.client = createClient(config);
    }


    private JestClient createClient(Configuration config) {
        JestClient client;
        final String scheme = config.get(ES_TRANSPORT_SCHEME);
        final Integer port = config.has(INDEX_PORT) ? config.get(INDEX_PORT) : HOST_PORT_DEFAULT;
        final List<String> serverUris = new ArrayList();
        String clientKey = "";
        for (String host : config.get(INDEX_HOSTS)) {
            String uri = scheme + "://" + host + ":" + port.toString();
            serverUris.add(uri);
            clientKey += uri;
        }
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig clientConfig = new HttpClientConfig.Builder(serverUris)
            .multiThreaded(true)
            .discoveryEnabled(config.has(CLIENT_SNIFF) && config.get(CLIENT_SNIFF))
            .discoveryFrequency(config.get(DISCOVERY_FREQUENCY), TimeUnit.SECONDS)
            .readTimeout(Math.max(config.get(TIMEOUT), config.get(METADATA_TIMEOUT)))
            .build();
        factory.setHttpClientConfig(clientConfig);
        return factory.getObject();
    }

    private static String formatException(String command, JestResult result) {
        return String.format(
            "Unknown ES error in %s. Status code is %d; message is %s",
            command,
            result.getResponseCode(),
            result.getErrorMessage()
        );
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
    private void checkForOrCreateIndex() throws BackendException {
        JestResult result;
        OpenIndex oi = new OpenIndex.Builder(indexName)
            .setParameter("master_timeout", this.metadataTimeout)
            .setParameter("timeout", this.metadataTimeout)
            .build();

        try {
            result = this.client.execute(oi);
        } catch (IOException e) {
            throw new TemporaryBackendException("ES transport error in OpenIndex");
        }
        if (!result.isSucceeded()) {
            if (result.getResponseCode() == 404) {
                /*
                 * Use aliases for operational support; actual index name is
                 * suffixed with the index creation time.
                 */
                Long now = System.currentTimeMillis();
                String localIndexName = this.indexName + ":" + now.toString();
                Map<String, Object> sb = new HashMap<String, Object>();
                sb.put("number_of_shards", this.configuration.get(SHARD_COUNT));
                sb.put("number_of_replicas", this.configuration.get(REPLICA_COUNT));

                CreateIndex ci = new CreateIndex.Builder(localIndexName)
                    .settings(sb)
                    .setParameter("master_timeout", metadataTimeout)
                    .setParameter("timeout", metadataTimeout)
                    .build();

                try {
                    result = this.client.execute(ci);
                } catch (IOException e) {
                    throw new TemporaryBackendException("ES transport error in CreateIndex");
                }
                if (!result.isSucceeded()) {
                    throw new PermanentBackendException(formatException("CreateIndex", result));
                }

                AddAliasMapping am = new AddAliasMapping.Builder(localIndexName, this.indexName)
                    .build();

                ModifyAliases ma = new ModifyAliases.Builder(am)
                    .setParameter("master_timeout", metadataTimeout)
                    .setParameter("timeout", metadataTimeout)
                    .build();

                try {
                    result = this.client.execute(ma);
                } catch (IOException e) {
                  log.error("Index {} is was orphaned by alias creation failure!", localIndexName);
                  throw new TemporaryBackendException("ES transport error in add alias");
                }
                if (!result.isSucceeded()) {
                  log.error("Index {} is was orphaned by alias creation failure!", localIndexName);
                  throw new PermanentBackendException(formatException("AddAlias", result));
                }
            } else {
                throw new PermanentBackendException(formatException("OpenIndex", result));
            }
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
        checkForOrCreateIndex();
        XContentBuilder mapping;
        String source;
        Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(
            map == Mapping.DEFAULT || AttributeUtil.isString(dataType),
            "Specified illegal mapping [%s] for data type [%s]",
            map,
            dataType
        );

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
                if (map == Mapping.DEFAULT) {
                    map = Mapping.TEXT;
                }

                log.debug("Registering string type for {} with mapping {}", key, map);
                mapping.field("type", "string");
                switch (map) {
                    case STRING:
                        mapping.field("index", "not_analyzed");
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
            source = mapping.string();
        } catch (IOException e) {
            throw new PermanentBackendException("Could not render json for put mapping request", e);
        }

        PutMapping putMapping = new PutMapping.Builder(this.indexName, store, source)
            .setParameter("master_timeout", this.metadataTimeout)
            .setParameter("timeout", this.metadataTimeout)
            .build();

        JestResult result;
        try {
            result = this.client.execute(putMapping);
        } catch (IOException e) {
            throw new TemporaryBackendException("ES transport error in PutMapping", e);
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
        String source;
        int requests = 0;
        Bulk.Builder bulk = new Bulk.Builder()
            .defaultIndex(this.indexName);
        for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
            String store = stores.getKey();
            for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                String id = entry.getKey();
                IndexMutation mutation = entry.getValue();
                assert mutation.isConsolidated();
                Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());
                if (mutation.hasDeletions()) {
                    requests++;
                    if (mutation.isDeleted()) {
                        log.trace("Deleting entire document {}", id);
                        Delete delete = new Delete.Builder(id).type(store).build();
                        bulk.addAction(delete);
                    } else {
                        try {
                          source = XContentFactory.jsonBuilder().startObject()
                              .field("lang", "groovy")
                              .field("script", deletionScript)
                              .field("params", getDeletionParams(informations, store, mutation))
                              .string();
                            log.trace("Adding script {}", deletionScript);
                        } catch (IOException e) {
                            throw new PermanentBackendException(
                                "Could not render json for mutate request", e);
                        }

                        Update update = new Update.Builder(source)
                            .type(store)
                            .id(id)
                            .build();

                        bulk.addAction(update);
                    }
                }

                if (mutation.hasAdditions()) {
                    requests++;
                    int ttl = mutation.determineTTL();
                    if (mutation.isNew()) {
                        log.trace("Adding entire document {}", id);
                        try {
                            source = getNewDocument(
                                mutation.getAdditions(),
                                informations.get(store),
                                ttl
                            ).string();
                        } catch (IOException e) {
                            throw new PermanentBackendException(
                                "Could not render json for mutate request", e);
                        }
                        Index index = new Index.Builder(source)
                            .type(store)
                            .id(id)
                            .build();

                        bulk.addAction(index);
                    } else {
                        Preconditions.checkArgument(
                            ttl == 0,
                            "Elasticsearch only supports TTL on new documents [%s]",
                            id
                        );
                        try {
                            XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                                .field("lang", "groovy")
                                .field("script", additionScript)
                                .field("params", getAdditionParams(informations, store, mutation));

                            if (!mutation.hasDeletions()) {
                                XContentBuilder doc = getNewDocument(
                                    mutation.getAdditions(),
                                    informations.get(store),
                                    ttl
                                );
                                json.field("upsert", doc);
                            }

                            source = json.string();
                            log.trace("Adding script {}", additionScript);
                        } catch (IOException e) {
                            throw new PermanentBackendException(
                                "Could not render json for mutate request", e);
                        }

                        Update update = new Update.Builder(source)
                            .type(store)
                            .id(id)
                            .build();

                        bulk.addAction(update);
                    }
                }
            }
        }

        if (requests > 0) {
            BulkResult result;
            bulk.setParameter("timeout", this.timeout);
            try {
                result = this.client.execute(bulk.build());
            } catch (IOException e) {
                throw new TemporaryBackendException("ES transport error in bulk mutate", e);
            }
            if (!result.isSucceeded()) {
                boolean actualFailure = false;
                for (BulkResult.BulkResultItem item : result.getItems()) {
                    if (null != item.error && item.status != 404) {
                        actualFailure = true;
                        log.warn(String.format(
                            "Mutate (%s) on item %s in index %s failed with code %d and error %s",
                            item.operation,
                            item.id,
                            item.index,
                            item.status,
                            item.error
                        ));
                    }
                }
                if (actualFailure) {
                    throw new PermanentBackendException(formatException("bulk mutate", result));
                }
            }
        }
    }

    private Map<String, Object> getDeletionParams(KeyInformation.IndexRetriever informations, String storename, IndexMutation mutation) throws PermanentBackendException {
        Map<String, Object> params = new HashMap<String, Object>();
        List<String> singles = new ArrayList<String>();
        params.put("singles", singles);
        List<List<Object>> lists = new ArrayList<List<Object>>();
        params.put("lists", lists);
        List<Object> op;
        for (IndexEntry deletion : mutation.getDeletions()) {
            KeyInformation keyInformation = informations.get(storename).get(deletion.field);
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    singles.add(deletion.field);
                    if (hasDualStringMapping(informations.get(storename, deletion.field))) {
                        singles.add(getDualMappingName(deletion.field));
                    }
                    break;
                case SET:
                case LIST:
                    String jsValue = convertToJsType(deletion.value);
                    op = new ArrayList<Object>(2);
                    op.add(deletion.field);
                    op.add(jsValue);
                    lists.add(op);
                    if (hasDualStringMapping(informations.get(storename, deletion.field))) {
                        op = new ArrayList<Object>(2);
                        op.add(getDualMappingName(deletion.field));
                        op.add(jsValue);
                        lists.add(op);
                    }
                    break;

            }
        }
        return params;
    }

    private Map<String, Object> getAdditionParams(KeyInformation.IndexRetriever informations, String storename, IndexMutation mutation) throws PermanentBackendException {
        Map<String, Object> params = new HashMap<String, Object>();
        List<List<Object>> singles = new ArrayList<List<Object>>();
        params.put("singles", singles);
        List<List<Object>> lists = new ArrayList<List<Object>>();
        params.put("lists", lists);
        List<Object> op;
        for (IndexEntry e : mutation.getAdditions()) {
            KeyInformation keyInformation = informations.get(storename).get(e.field);
            String jsValue = convertToJsType(e.value);
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    op = new ArrayList<Object>(2);
                    op.add(e.field);
                    op.add(jsValue);
                    singles.add(op);
                    if (hasDualStringMapping(keyInformation)) {
                        op = new ArrayList<Object>(2);
                        op.add(getDualMappingName(e.field));
                        op.add(jsValue);
                        singles.add(op);
                    }
                    break;
                case SET:
                case LIST:
                    op = new ArrayList<Object>(2);
                    op.add(e.field);
                    op.add(jsValue);
                    lists.add(op);
                    if (hasDualStringMapping(keyInformation)) {
                        op = new ArrayList<Object>(2);
                        op.add(getDualMappingName(e.field));
                        op.add(jsValue);
                        lists.add(op);
                    }
                    break;
            }
        }
        return params;
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
        int requests = 0;
        Bulk.Builder bulk = new Bulk.Builder()
            .defaultIndex(this.indexName);

        for (Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
            String store = stores.getKey();
            for (Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                String id = entry.getKey();
                List<IndexEntry> content = entry.getValue();
                if (content == null || content.size() == 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("Deleting entire document {}", id);
                    }

                    Delete delete = new Delete.Builder(id).type(store).build();
                    bulk.addAction(delete);
                    requests++;
                } else {
                    String source;
                    if (log.isTraceEnabled()) {
                        log.trace("Adding entire document {}", id);
                    }
                    try {
                        source = getNewDocument(
                            content,
                            informations.get(store),
                            IndexMutation.determineTTL(content)
                        ).string();
                    } catch (IOException e) {
                        throw new PermanentBackendException(
                            "Could not render json for restore request", e);
                    }
                    Index index = new Index.Builder(source)
                        .type(store)
                        .id(id)
                        .build();

                    bulk.addAction(index);
                    requests++;
                }
            }
        }

        if (requests > 0) {
            JestResult result;
            bulk.setParameter("timeout", this.timeout);
            try {
                result = this.client.execute(bulk.build());
            } catch (IOException e) {
                throw new TemporaryBackendException(
                    "ES transport exception during bulk restore", e);
            }
            if (!result.isSucceeded()) {
                throw new PermanentBackendException(formatException("bulk restore", result));
            }
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
                if (map==Mapping.TEXT && !janusgraphPredicate.toString().startsWith("CONTAINS"))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + janusgraphPredicate);
                if (map==Mapping.STRING && janusgraphPredicate.toString().startsWith("CONTAINS"))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + janusgraphPredicate);
                if (map==Mapping.TEXTSTRING && !janusgraphPredicate.toString().startsWith("CONTAINS"))
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
        String type = query.getStore();
        SearchSourceBuilder ssb = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .postFilter(getFilter(query.getCondition(),informations.get(type)))
            .from(0)
            .size(query.hasLimit() ? query.getLimit() : this.maxResultsSize)
            .noFields();

        if (!query.getOrder().isEmpty()) {
            List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (int i = 0; i < orders.size(); i++) {
                IndexQuery.OrderEntry orderEntry = orders.get(i);
                String unmapped = convertToEsDataType(orderEntry.getDatatype());
                FieldSortBuilder fsb = new FieldSortBuilder(orderEntry.getKey())
                    .order(orderEntry.getOrder() == Order.ASC ? SortOrder.ASC : SortOrder.DESC)
                    .unmappedType(unmapped);

                ssb.sort(fsb);
            }
        }

        Search search = new Search.Builder(ssb.toString())
            .setParameter("timeout", this.timeout)
            .addType(type)
            .addIndex(this.indexName)
            .build();

        JestResult result;
        try {
            result = this.client.execute(search);
        } catch (IOException e) {
            throw new TemporaryBackendException("ES transport error in search", e);
        }
        if (!result.isSucceeded()) {
            throw new PermanentBackendException(formatException("search", result));
        }

        JsonArray hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
        List<String> results = new ArrayList<String>(hits.size());
        for (JsonElement hit : hits) {
            results.add(hit.getAsJsonObject().get("_id").getAsString());
        }

        return results;
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
        SearchSourceBuilder ssb = new SearchSourceBuilder()
            .query(QueryBuilders.queryStringQuery(query.getQuery()))
            .from(query.getOffset())
            .size(query.hasLimit() ? query.getLimit() : this.maxResultsSize)
            .noFields();

        Search search = new Search.Builder(ssb.toString())
            .setParameter("timeout", this.timeout)
            .addType(query.getStore())
            .addIndex(this.indexName)
            .build();

        JestResult result;
        try {
            result = this.client.execute(search);
        } catch (IOException e) {
            throw new TemporaryBackendException("ES transport error in search", e);
        }
        if (!result.isSucceeded()) {
            throw new PermanentBackendException(formatException("search", result));
        }

        JsonArray hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
        List<RawQuery.Result<String>> results = new ArrayList<RawQuery.Result<String>>(hits.size());
        for (JsonElement hit : hits) {
            JsonObject obj = hit.getAsJsonObject();
            results.add(new RawQuery.Result<String>(
                obj.get("_id").getAsString(),
                obj.get("_score").getAsDouble()
            ));
        }

        return results;
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
      client.shutdownClient();
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
          Delete deleteIndex = new Delete.Builder(indexName).build();
          client.execute(deleteIndex);
        } catch (Exception e) {
            throw new PermanentBackendException("Could not delete index " + indexName, e);
        } finally {
            close();
        }
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
