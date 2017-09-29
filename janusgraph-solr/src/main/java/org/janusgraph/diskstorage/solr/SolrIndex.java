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

package org.janusgraph.diskstorage.solr;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.KeeperException;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
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
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.solr.transform.GeoToWktConverter;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * @author Jared Holmberg (jholmberg@bericotechnoLogies.com), Pavel Yaskevich (pavel@thinkaurelius.com)
 */
@PreInitializeConfigOptions
public class SolrIndex implements IndexProvider {

    private static final Logger logger = LoggerFactory.getLogger(SolrIndex.class);


    private static final String DEFAULT_ID_FIELD = "id";

    private enum Mode {
        HTTP, CLOUD;

        public static Mode parse(String mode) {
            for (final Mode m : Mode.values()) {
                if (m.toString().equalsIgnoreCase(mode)) return m;
            }
            throw new IllegalArgumentException("Unrecognized mode: "+mode);
        }

    }

    public static final ConfigNamespace SOLR_NS =
            new ConfigNamespace(INDEX_NS, "solr", "Solr index configuration");

    public static final ConfigOption<String> SOLR_MODE = new ConfigOption<String>(SOLR_NS,"mode",
            "The operation mode for Solr which is either via HTTP (`http`) or using SolrCloud (`cloud`)",
            ConfigOption.Type.GLOBAL_OFFLINE, "cloud");

    public static final ConfigOption<Boolean> DYNAMIC_FIELDS = new ConfigOption<Boolean>(SOLR_NS,"dyn-fields",
            "Whether to use dynamic fields (which appends the data type to the field name). If dynamic fields is disabled" +
                    "the user must map field names and define them explicitly in the schema.",
            ConfigOption.Type.GLOBAL_OFFLINE, true);

    public static final ConfigOption<String[]> KEY_FIELD_NAMES = new ConfigOption<String[]>(SOLR_NS,"key-field-names",
            "Field name that uniquely identifies each document in Solr. Must be specified as a list of `collection=field`.",
            ConfigOption.Type.GLOBAL, String[].class);

    public static final ConfigOption<String> TTL_FIELD = new ConfigOption<String>(SOLR_NS,"ttl_field",
            "Name of the TTL field for Solr collections.",
            ConfigOption.Type.GLOBAL_OFFLINE, "ttl");

    /** SolrCloud Configuration */

    public static final ConfigOption<String> ZOOKEEPER_URL = new ConfigOption<String>(SOLR_NS,"zookeeper-url",
            "URL of the Zookeeper instance coordinating the SolrCloud cluster",
            ConfigOption.Type.MASKABLE, "localhost:2181");

    public static final ConfigOption<Integer> NUM_SHARDS = new ConfigOption<Integer>(SOLR_NS,"num-shards",
            "Number of shards for a collection. This applies when creating a new collection which is only supported under the SolrCloud operation mode.",
            ConfigOption.Type.GLOBAL_OFFLINE, 1);

    public static final ConfigOption<Integer> MAX_SHARDS_PER_NODE = new ConfigOption<Integer>(SOLR_NS,"max-shards-per-node",
            "Maximum number of shards per node. This applies when creating a new collection which is only supported under the SolrCloud operation mode.",
            ConfigOption.Type.GLOBAL_OFFLINE, 1);

    public static final ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<Integer>(SOLR_NS,"replication-factor",
            "Replication factor for a collection. This applies when creating a new collection which is only supported under the SolrCloud operation mode.",
            ConfigOption.Type.GLOBAL_OFFLINE, 1);

    public static final ConfigOption<String> SOLR_DEFAULT_CONFIG = new ConfigOption<String>(SOLR_NS,"configset",
            "If specified, the same solr configSet can be resued for each new Collection that is created in SolrCloud.",
            ConfigOption.Type.MASKABLE, String.class);


    /** HTTP Configuration */

    public static final ConfigOption<String[]> HTTP_URLS = new ConfigOption<String[]>(SOLR_NS,"http-urls",
            "List of URLs to use to connect to Solr Servers (LBHttpSolrClient is used), don't add core or collection name to the URL.",
            ConfigOption.Type.MASKABLE, new String[] { "http://localhost:8983/solr" });

    public static final ConfigOption<Integer> HTTP_CONNECTION_TIMEOUT = new ConfigOption<Integer>(SOLR_NS,"http-connection-timeout",
            "Solr HTTP connection timeout.",
            ConfigOption.Type.MASKABLE, 5000);

    public static final ConfigOption<Boolean> HTTP_ALLOW_COMPRESSION = new ConfigOption<Boolean>(SOLR_NS,"http-compression",
            "Enable/disable compression on the HTTP connections made to Solr.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Integer> HTTP_MAX_CONNECTIONS_PER_HOST = new ConfigOption<Integer>(SOLR_NS,"http-max-per-host",
            "Maximum number of HTTP connections per Solr host.",
            ConfigOption.Type.MASKABLE, 20);

    public static final ConfigOption<Integer> HTTP_GLOBAL_MAX_CONNECTIONS = new ConfigOption<Integer>(SOLR_NS,"http-max",
            "Maximum number of HTTP connections in total to all Solr servers.",
            ConfigOption.Type.MASKABLE, 100);

    public static final ConfigOption<Boolean> WAIT_SEARCHER = new ConfigOption<Boolean>(SOLR_NS, "wait-searcher",
            "When mutating - wait for the index to reflect new mutations before returning. This can have a negative impact on performance.",
            ConfigOption.Type.LOCAL, false);


    private static final IndexFeatures SOLR_FEATURES = new IndexFeatures.Builder()
        .supportsDocumentTTL()
        .setDefaultStringMapping(Mapping.TEXT)
        .supportedStringMappings(Mapping.TEXT, Mapping.STRING)
        .supportsCardinality(Cardinality.SINGLE)
        .supportsCardinality(Cardinality.LIST)
        .supportsCardinality(Cardinality.SET)
        .supportsCustomAnalyzer()
        .supportsGeoContains()
        .build();

    private static Map<Geo, String> SPATIAL_PREDICATES = spatialPredicates();

    private final SolrClient solrClient;
    private final Configuration configuration;
    private final Mode mode;
    private final boolean dynFields;
    private final Map<String, String> keyFieldIds;
    private final String ttlField;
    private final int batchSize;
    private final boolean waitSearcher;

    public SolrIndex(final Configuration config) throws BackendException {
        Preconditions.checkArgument(config!=null);
        configuration = config;

        mode = Mode.parse(config.get(SOLR_MODE));
        dynFields = config.get(DYNAMIC_FIELDS);
        keyFieldIds = parseKeyFieldsForCollections(config);
        batchSize = config.get(INDEX_MAX_RESULT_SET_SIZE);
        ttlField = config.get(TTL_FIELD);
        waitSearcher = config.get(WAIT_SEARCHER);

        if (mode==Mode.CLOUD) {
            final String zookeeperUrl = config.get(SolrIndex.ZOOKEEPER_URL);
            final CloudSolrClient cloudServer = new CloudSolrClient.Builder()
                .withZkHost(zookeeperUrl)
                .sendUpdatesOnlyToShardLeaders()
                .build();
            cloudServer.connect();
            solrClient = cloudServer;
        } else if (mode==Mode.HTTP) {
            final HttpClient clientParams = HttpClientUtil.createClient(new ModifiableSolrParams() {{
                add(HttpClientUtil.PROP_ALLOW_COMPRESSION, config.get(HTTP_ALLOW_COMPRESSION).toString());
                add(HttpClientUtil.PROP_CONNECTION_TIMEOUT, config.get(HTTP_CONNECTION_TIMEOUT).toString());
                add(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, config.get(HTTP_MAX_CONNECTIONS_PER_HOST).toString());
                add(HttpClientUtil.PROP_MAX_CONNECTIONS, config.get(HTTP_GLOBAL_MAX_CONNECTIONS).toString());
            }});

            solrClient = new LBHttpSolrClient.Builder()
                .withHttpClient(clientParams)
                .withBaseSolrUrls(config.get(HTTP_URLS))
                .build();


        } else {
            throw new IllegalArgumentException("Unsupported Solr operation mode: " + mode);
        }
    }

    private Map<String, String> parseKeyFieldsForCollections(Configuration config) throws BackendException {
        final Map<String, String> keyFieldNames = new HashMap<String, String>();
        final String[] collectionFieldStatements = config.has(KEY_FIELD_NAMES) ? config.get(KEY_FIELD_NAMES) : new String[0];
        for (final String collectionFieldStatement : collectionFieldStatements) {
            final String[] parts = collectionFieldStatement.trim().split("=");
            if (parts.length != 2) {
                throw new PermanentBackendException("Unable to parse the collection name / key field name pair. It should be of the format collection=field");
            }
            final String collectionName = parts[0];
            final String keyFieldName = parts[1];
            keyFieldNames.put(collectionName, keyFieldName);
        }
        return keyFieldNames;
    }

    private String getKeyFieldId(String collection) {
        String field = keyFieldIds.get(collection);
        if (field==null) field = DEFAULT_ID_FIELD;
        return field;
    }

    /**
     * Unlike the ElasticSearch Index, which is schema free, Solr requires a schema to
     * support searching. This means that you will need to modify the solr schema with the
     * appropriate field definitions in order to work properly.  If you have a running instance
     * of Solr and you modify its schema with new fields, don't forget to re-index!
     * @param store Index store
     * @param key New key to register
     * @param information Datatype to register for the key
     * @param tx enclosing transaction
     * @throws org.janusgraph.diskstorage.BackendException
     */
    @SuppressWarnings("unchecked")
    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        if (mode==Mode.CLOUD) {
            final CloudSolrClient client = (CloudSolrClient) solrClient;
            try {
                createCollectionIfNotExists(client, configuration, store);
            } catch (final IOException | SolrServerException | InterruptedException | KeeperException e) {
                throw new PermanentBackendException(e);
            }
        }
        //Since all data types must be defined in the schema.xml, pre-registering a type does not work
        //But we check Analyse feature
        String analyzer = (String) ParameterType.STRING_ANALYZER.findParameter(information.getParameters(), null);
        if (analyzer != null) {
            //If the key have a tokenizer, we try to get it by reflextion
            try {
                ((Constructor<Tokenizer>) ClassLoader.getSystemClassLoader().loadClass(analyzer)
                        .getConstructor()).newInstance();
            } catch (final ReflectiveOperationException e) {
                throw new PermanentBackendException(e.getMessage(),e);
            }
        }
        analyzer = (String) ParameterType.TEXT_ANALYZER.findParameter(information.getParameters(), null);
        if (analyzer != null) {
            //If the key have a tokenizer, we try to get it by reflextion
            try {
                ((Constructor<Tokenizer>) ClassLoader.getSystemClassLoader().loadClass(analyzer)
                        .getConstructor()).newInstance();
            } catch (final ReflectiveOperationException e) {
                throw new PermanentBackendException(e.getMessage(),e);
            }
        }
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        logger.debug("Mutating SOLR");
        try {
            for (final Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                final String collectionName = stores.getKey();
                final String keyIdField = getKeyFieldId(collectionName);

                final List<String> deleteIds = new ArrayList<String>();
                final Collection<SolrInputDocument> changes = new ArrayList<SolrInputDocument>();

                for (final Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    final String docId = entry.getKey();
                    final IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());

                    //Handle any deletions
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            logger.trace("Deleting entire document {}", docId);
                            deleteIds.add(docId);
                        } else {
                            final List<IndexEntry> fieldDeletions = new ArrayList<IndexEntry>(mutation.getDeletions());
                            if (mutation.hasAdditions()) {
                                for (final IndexEntry indexEntry : mutation.getAdditions()) {
                                    fieldDeletions.remove(indexEntry);
                                }
                            }
                            handleRemovalsFromIndex(collectionName, keyIdField, docId, fieldDeletions, informations);
                        }
                    }

                    if (mutation.hasAdditions()) {
                        final int ttl = mutation.determineTTL();

                        final SolrInputDocument doc = new SolrInputDocument();
                        doc.setField(keyIdField, docId);

                        final boolean isNewDoc = mutation.isNew();

                        if (isNewDoc)
                            logger.trace("Adding new document {}", docId);
                        final Map<String, Object> adds = collectFieldValues(mutation.getAdditions(), collectionName, informations);
                        // If cardinality is not single then we should use the "add" operation to update
                        // the index so we don't overwrite existing values.
                        adds.keySet().stream().forEach(v-> {
                            final KeyInformation keyInformation = informations.get(collectionName, v);
                            final String solrOp = keyInformation.getCardinality() == Cardinality.SINGLE ? "set" : "add";
                            doc.setField(v, isNewDoc ? adds.get(v) :
                                new HashMap<String, Object>(1) {{put(solrOp, adds.get(v));}}
                            );
                        });
                        if (ttl>0) {
                            Preconditions.checkArgument(isNewDoc,"Solr only supports TTL on new documents [%s]",docId);
                            doc.setField(ttlField, String.format("+%dSECONDS", ttl));
                        }
                        changes.add(doc);
                    }
                }

                commitDeletes(collectionName, deleteIds);
                commitDocumentChanges(collectionName, changes);
            }
        } catch (final IllegalArgumentException e) {
            throw new PermanentBackendException("Unable to complete query on Solr.", e);
        } catch (final Exception e) {
            throw storageException(e);
        }
    }

    private void handleRemovalsFromIndex(String collectionName, String keyIdField, String docId, List<IndexEntry> fieldDeletions, KeyInformation.IndexRetriever informations) throws SolrServerException, IOException, BackendException {
        final Map<String, String> fieldDeletes = new HashMap<String, String>(1) {{ put("set", null); }};
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(keyIdField, docId);
        for(final IndexEntry v: fieldDeletions) {
            final KeyInformation keyInformation = informations.get(collectionName, v.field);
            // If the cardinality is a Set or List, we just need to remove the individual value
            // received in the mutation and not set the field to null, but we still consolidate the values
            // in the event of multiple removals in one mutation.
            final Map<String, Object> deletes = collectFieldValues(fieldDeletions, collectionName, informations);
            deletes.keySet().stream().forEach(vertex-> {
                doc.setField(vertex, keyInformation.getCardinality() == Cardinality.SINGLE?
                        fieldDeletes:new HashMap<String, Object>(1) {{ put("remove", deletes.get(vertex));}}
                        );
            });
        }

        final UpdateRequest singleDocument = newUpdateRequest();
        singleDocument.add(doc);
        solrClient.request(singleDocument, collectionName);

    }

    private Object convertValue(Object value) throws BackendException {
        if (value instanceof Geoshape) {
            return GeoToWktConverter.convertToWktString((Geoshape) value);
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if(value instanceof Instant) {
            if(Math.floorMod(((Instant) value).getNano(), 1000000) != 0) {
                throw new IllegalArgumentException("Solr indexes do not support nanoseconds");
            }
            return new Date(((Instant) value).toEpochMilli());
        }
        return value;
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        try {
            for (final Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final String collectionName = stores.getKey();

                final List<String> deleteIds = new ArrayList<String>();
                final List<SolrInputDocument> newDocuments = new ArrayList<SolrInputDocument>();

                for (final Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();

                    if (content == null || content.isEmpty()) {
                        if (logger.isTraceEnabled())
                            logger.trace("Deleting document [{}]", docID);

                        deleteIds.add(docID);
                        continue;
                    }
                    final SolrInputDocument doc = new SolrInputDocument();
                    doc.setField(getKeyFieldId(collectionName), docID);
                    final Map<String, Object> adds = collectFieldValues(content, collectionName, informations);
                    adds.entrySet().stream().forEach(e -> doc.setField(e.getKey(), e.getValue()));
                    newDocuments.add(doc);
                }
                commitDeletes(collectionName, deleteIds);
                commitDocumentChanges(collectionName, newDocuments);
            }
        } catch (final Exception e) {
            throw new TemporaryBackendException("Could not restore Solr index", e);
        }
    }

    // This method will create a map of field ids to values.  In the case of multiValued fields,
    // it will consolidate all the values into one List or Set so it can be updated with a single Solr operation
    private Map<String, Object> collectFieldValues(List<IndexEntry> content, String collectionName, KeyInformation.IndexRetriever informations) throws BackendException {
        final Map<String, Object> docs = new HashMap<>();
        for (final IndexEntry addition: content) {
            final KeyInformation keyInformation = informations.get(collectionName, addition.field);
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    docs.put(addition.field, convertValue(addition.value));
                    break;
                case SET:
                    if (!docs.containsKey(addition.field)) {
                        docs.put(addition.field, new HashSet<Object>());
                    }
                    ((Set<Object>) docs.get(addition.field)).add(convertValue(addition.value));
                    break;
                case LIST:
                    if (!docs.containsKey(addition.field)) {
                        docs.put(addition.field,  new ArrayList<Object>());
                    }
                    ((List<Object>) docs.get(addition.field)).add(convertValue(addition.value));
                    break;
            }
        }
        return docs;
    }

    private void commitDocumentChanges(String collectionName, Collection<SolrInputDocument> documents) throws SolrServerException, IOException {
        if (documents.size() == 0) return;

        try {
            solrClient.request(newUpdateRequest().add(documents), collectionName);
        } catch (final HttpSolrClient.RemoteSolrException rse) {
            logger.error("Unable to save documents to Solr as one of the shape objects stored were not compatible with Solr.", rse);
            logger.error("Details in failed document batch: ");
            for (final SolrInputDocument d : documents) {
                final Collection<String> fieldNames = d.getFieldNames();
                for (final String name : fieldNames) {
                    logger.error(name + ":" + d.getFieldValue(name));
                }
            }

            throw rse;
        }
    }

    private void commitDeletes(String collectionName, List<String> deleteIds) throws SolrServerException, IOException {
        if (deleteIds.size() == 0) return;
        solrClient.request(newUpdateRequest().deleteById(deleteIds), collectionName);
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        final String collection = query.getStore();
        final String keyIdField = getKeyFieldId(collection);
        final SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.set(CommonParams.FL, keyIdField);
        final String queryFilter = buildQueryFilter(query.getCondition(), informations.get(collection));
        solrQuery.addFilterQuery(queryFilter);
        if (!query.getOrder().isEmpty()) {
            final List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (final IndexQuery.OrderEntry order1 : orders) {
                final String item = order1.getKey();
                final SolrQuery.ORDER order = order1.getOrder() == Order.ASC ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
                solrQuery.addSort(new SolrQuery.SortClause(item, order));
            }
        }
        solrQuery.setStart(0);
        if (query.hasLimit()) {
            solrQuery.setRows(Math.min(query.getLimit(), batchSize));
        } else {
            solrQuery.setRows(batchSize);
        }
        return executeQuery(query.hasLimit() ? query.getLimit() : null, 0, collection, solrQuery, doc -> doc.getFieldValue(keyIdField).toString());
    }

    private <E> Stream<E> executeQuery(Integer limit, int offset, String collection, SolrQuery solrQuery, Function<SolrDocument, E> function) throws PermanentBackendException {
        try {
            final SolrResultIterator<E> resultIterator = new SolrResultIterator<>(solrClient, limit, offset, solrQuery.getRows(), collection, solrQuery, function);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false);
        } catch (final IOException | UncheckedIOException e) {
            logger.error("Query did not complete : ", e);
            throw new PermanentBackendException(e);
        } catch (final SolrServerException | UncheckedSolrException e) {
            logger.error("Unable to query Solr index.", e);
            throw new PermanentBackendException(e);
        }
    }


    private SolrQuery runCommonQuery(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx, String collection, String keyIdField) throws BackendException {
        final SolrQuery solrQuery = new SolrQuery(query.getQuery())
                                .addField(keyIdField)
                                .setIncludeScore(true)
                                .setStart(query.getOffset());
        if (query.hasLimit()) {
            solrQuery.setRows(Math.min(query.getLimit(), batchSize));
        } else {
            solrQuery.setRows(batchSize);
        }

        for(final Parameter parameter: query.getParameters()) {
            if (parameter.value() instanceof String[]) {
                solrQuery.setParam(parameter.key(), (String[]) parameter.value());
            } else if (parameter.value() instanceof String) {
                solrQuery.setParam(parameter.key(), (String) parameter.value());
            }
        }
        return solrQuery;
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        final String collection = query.getStore();
        final String keyIdField = getKeyFieldId(collection);
        return executeQuery(query.hasLimit() ? query.getLimit() : null, query.getOffset(), collection, runCommonQuery(query, informations, tx, collection, keyIdField), doc -> {
            final double score = Double.parseDouble(doc.getFieldValue("score").toString());
            return new RawQuery.Result<>(doc.getFieldValue(keyIdField).toString(), score);
        });
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        try {
            final String collection = query.getStore();
            final String keyIdField = getKeyFieldId(collection);
            final QueryResponse response = solrClient.query(collection, runCommonQuery(query, informations, tx, collection, keyIdField));
            logger.debug("Executed query [{}] in {} ms", query.getQuery(), response.getElapsedTime());
            return response.getResults().getNumFound();
        } catch (final IOException e) {
            logger.error("Query did not complete : ", e);
            throw new PermanentBackendException(e);
        } catch (final SolrServerException e) {
            logger.error("Unable to query Solr index.", e);
            throw new PermanentBackendException(e);
        }
    }

    private static String escapeValue(Object value) {
        return ClientUtils.escapeQueryChars(value.toString());
    }

    public String buildQueryFilter(Condition<JanusGraphElement> condition, KeyInformation.StoreRetriever informations) {
        if (condition instanceof PredicateCondition) {
            final PredicateCondition<String, JanusGraphElement> atom = (PredicateCondition<String, JanusGraphElement>) condition;
            final Object value = atom.getValue();
            final String key = atom.getKey();
            final JanusGraphPredicate janusgraphPredicate = atom.getPredicate();

            if (value instanceof Number) {
                final String queryValue = escapeValue(value);
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on numeric types: " + janusgraphPredicate);
                final Cmp numRel = (Cmp) janusgraphPredicate;
                switch (numRel) {
                    case EQUAL:
                        return (key + ":" + queryValue);
                    case NOT_EQUAL:
                        return ("-" + key + ":" + queryValue);
                    case LESS_THAN:
                        //use right curly to mean up to but not including value
                        return (key + ":[* TO " + queryValue + "}");
                    case LESS_THAN_EQUAL:
                        return (key + ":[* TO " + queryValue + "]");
                    case GREATER_THAN:
                        //use left curly to mean greater than but not including value
                        return (key + ":{" + queryValue + " TO *]");
                    case GREATER_THAN_EQUAL:
                        return (key + ":[" + queryValue + " TO *]");
                    default: throw new IllegalArgumentException("Unexpected relation: " + numRel);
                }
            } else if (value instanceof String) {
                final Mapping map = getStringMapping(informations.get(key));
                assert map==Mapping.TEXT || map==Mapping.STRING;
                if (map==Mapping.TEXT && !Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + janusgraphPredicate);
                if (map==Mapping.STRING && Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + janusgraphPredicate);

                //Special case
                if (janusgraphPredicate == Text.CONTAINS) {
                    return tokenize(informations, value, key, janusgraphPredicate,  (String) ParameterType.TEXT_ANALYZER.findParameter(informations.get(key).getParameters(), null));
                } else if (janusgraphPredicate == Text.PREFIX || janusgraphPredicate == Text.CONTAINS_PREFIX) {
                    return (key + ":" + escapeValue(value) + "*");
                } else if (janusgraphPredicate == Text.REGEX || janusgraphPredicate == Text.CONTAINS_REGEX) {
                    return (key + ":/" + value + "/");
                } else if (janusgraphPredicate == Cmp.EQUAL) {
                    final String tokenizer = (String) ParameterType.STRING_ANALYZER.findParameter(informations.get(key).getParameters(), null);
                    if(tokenizer != null){
                        return tokenize(informations, value, key, janusgraphPredicate,tokenizer);
                    } else {
                        return (key + ":\"" + escapeValue(value) + "\"");
                    }
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return ("-" + key + ":\"" + escapeValue(value) + "\"");
                } else if (janusgraphPredicate == Text.FUZZY || janusgraphPredicate == Text.CONTAINS_FUZZY) {
                    return (key + ":"+escapeValue(value)+"~");
                } else {
                    throw new IllegalArgumentException("Relation is not supported for string value: " + janusgraphPredicate);
                }
            } else if (value instanceof Geoshape) {
                final Mapping map = Mapping.getMapping(informations.get(key));
                Preconditions.checkArgument(janusgraphPredicate instanceof Geo && janusgraphPredicate != Geo.DISJOINT, "Relation not supported on geo types: " + janusgraphPredicate);
                Preconditions.checkArgument(map == Mapping.PREFIX_TREE || janusgraphPredicate == Geo.WITHIN || janusgraphPredicate == Geo.INTERSECT, "Relation not supported on geopoint types: " + janusgraphPredicate);
                final Geoshape geo = (Geoshape)value;
                if (geo.getType() == Geoshape.Type.CIRCLE && (janusgraphPredicate == Geo.INTERSECT || map == Mapping.DEFAULT)) {
                    final Geoshape.Point center = geo.getPoint();
                    return ("{!geofilt sfield=" + key +
                            " pt=" + center.getLatitude() + "," + center.getLongitude() +
                            " d=" + geo.getRadius() + "} distErrPct=0"); //distance in kilometers
                } else if (geo.getType() == Geoshape.Type.BOX && (janusgraphPredicate == Geo.INTERSECT || map == Mapping.DEFAULT)) {
                    final Geoshape.Point southwest = geo.getPoint(0);
                    final Geoshape.Point northeast = geo.getPoint(1);
                    return (key + ":[" + southwest.getLatitude() + "," + southwest.getLongitude() +
                            " TO " + northeast.getLatitude() + "," + northeast.getLongitude() + "]");
                } else if (map == Mapping.PREFIX_TREE) {
                    final StringBuilder builder = new StringBuilder(key + ":\"");
                    builder.append(SPATIAL_PREDICATES.get(janusgraphPredicate) + "(");
                    builder.append(geo + ")\" distErrPct=0");
                    return builder.toString();
                } else {
                    throw new IllegalArgumentException("Unsupported or invalid search shape type: " + geo.getType());
                }
            } else if (value instanceof Date || value instanceof Instant) {
                final String s = value.toString();
                final String queryValue = escapeValue(value instanceof Date ? toIsoDate((Date) value) : value.toString());
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on date types: " + janusgraphPredicate);
                final Cmp numRel = (Cmp) janusgraphPredicate;

                switch (numRel) {
                    case EQUAL:
                        return (key + ":" + queryValue);
                    case NOT_EQUAL:
                        return ("-" + key + ":" + queryValue);
                    case LESS_THAN:
                        //use right curly to mean up to but not including value
                        return (key + ":[* TO " + queryValue + "}");
                    case LESS_THAN_EQUAL:
                        return (key + ":[* TO " + queryValue + "]");
                    case GREATER_THAN:
                        //use left curly to mean greater than but not including value
                        return (key + ":{" + queryValue + " TO *]");
                    case GREATER_THAN_EQUAL:
                        return (key + ":[" + queryValue + " TO *]");
                    default: throw new IllegalArgumentException("Unexpected relation: " + numRel);
                }
            } else if (value instanceof Boolean) {
                final Cmp numRel = (Cmp) janusgraphPredicate;
                final String queryValue = escapeValue(value);
                switch (numRel) {
                    case EQUAL:
                        return (key + ":" + queryValue);
                    case NOT_EQUAL:
                        return ("-" + key + ":" + queryValue);
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }
            } else if (value instanceof UUID) {
                if (janusgraphPredicate == Cmp.EQUAL) {
                    return (key + ":\"" + escapeValue(value) + "\"");
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return ("-" + key + ":\"" + escapeValue(value) + "\"");
                } else {
                    throw new IllegalArgumentException("Relation is not supported for uuid value: " + janusgraphPredicate);
                }
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        } else if (condition instanceof Not) {
            final String sub = buildQueryFilter(((Not)condition).getChild(),informations);
            if (StringUtils.isNotBlank(sub)) return "-("+sub+")";
            else return "";
        } else if (condition instanceof And) {
            final int numChildren = ((And) condition).size();
            final StringBuilder sb = new StringBuilder();
            for (final Condition<JanusGraphElement> c : condition.getChildren()) {
                final String sub = buildQueryFilter(c, informations);

                if (StringUtils.isBlank(sub))
                    continue;

                // we don't have to add "+" which means AND iff
                // a. it's a NOT query,
                // b. expression is a single statement in the AND.
                if (!sub.startsWith("-") && numChildren > 1)
                    sb.append("+");

                sb.append(sub).append(" ");
            }
            return sb.toString();
        } else if (condition instanceof Or) {
            final StringBuilder sb = new StringBuilder();
            int element=0;
            for (final Condition<JanusGraphElement> c : condition.getChildren()) {
                final String sub = buildQueryFilter(c,informations);
                if (StringUtils.isBlank(sub)) continue;
                if (element==0) sb.append("(");
                else sb.append(" OR ");
                sb.append(sub);
                element++;
            }
            if (element>0) sb.append(")");
            return sb.toString();
        } else {
            throw new IllegalArgumentException("Invalid condition: " + condition);
        }
    }

    private String tokenize(KeyInformation.StoreRetriever informations, Object value, String key,
            JanusGraphPredicate janusgraphPredicate, String tokenizer) {
        List<String> terms;
        if(tokenizer != null){
            terms = customTokenize(tokenizer, (String) value);
        } else {
            terms = Text.tokenize((String) value);
        }
        if (terms.isEmpty()) {
            return "";
        } else if (terms.size() == 1) {
            return (key + ":(" + escapeValue(terms.get(0)) + ")");
        } else {
            final And<JanusGraphElement> andTerms = new And<JanusGraphElement>();
            for (final String term : terms) {
                andTerms.add(new PredicateCondition<String, JanusGraphElement>(key, janusgraphPredicate, term));
            }
            return buildQueryFilter(andTerms, informations);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> customTokenize(String tokenizerClass, String value){
        CachingTokenFilter stream = null;
        try {
            final List<String> terms = new ArrayList<>();
            final Tokenizer tokenizer = ((Constructor<Tokenizer>) ClassLoader.getSystemClassLoader().loadClass(tokenizerClass)
                    .getConstructor()).newInstance();
            tokenizer.setReader(new StringReader(value));
            stream = new CachingTokenFilter(tokenizer);
            final TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                terms.add(termAtt.getBytesRef().utf8ToString());
            }
            return terms;
        } catch ( ReflectiveOperationException | IOException e) {
                throw new IllegalArgumentException(e.getMessage(),e);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    private String toIsoDate(Date value) {
        final TimeZone tz = TimeZone.getTimeZone("UTC");
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(tz);
        return df.format(value);
    }

    /**
     * Solr handles all transactions on the server-side. That means all
     * commit, optimize, or rollback applies since the last commit/optimize/rollback.
     * Solr documentation recommends best way to update Solr is in one process to avoid
     * race conditions.
     *
     * @return New Transaction Handle
     * @throws org.janusgraph.diskstorage.BackendException
     */
    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new DefaultTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        logger.trace("Shutting down connection to Solr", solrClient);
        try {
            solrClient.close();
        } catch (final IOException e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            if (mode!=Mode.CLOUD) {
                logger.error("Operation only supported for SolrCloud. Cores must be deleted manually through the Solr API when using HTTP mode.");
                return;
            }
            logger.debug("Clearing storage from Solr: {}", solrClient);
            final ZkStateReader zkStateReader = ((CloudSolrClient) solrClient).getZkStateReader();
            zkStateReader.forciblyRefreshAllClusterStateSlow();
            final ClusterState clusterState = zkStateReader.getClusterState();
            for (final String collection : clusterState.getCollectionsMap().keySet()) {
                logger.debug("Clearing collection [{}] in Solr",collection);
                // Collection is not dropped because it may have been created externally
                final UpdateRequest deleteAll = newUpdateRequest();
                deleteAll.deleteByQuery("*:*");
                solrClient.request(deleteAll, collection);
            }

        } catch (final SolrServerException e) {
            logger.error("Unable to clear storage from index due to server error on Solr.", e);
            throw new PermanentBackendException(e);
        } catch (final IOException e) {
            logger.error("Unable to clear storage from index due to low-level I/O error.", e);
            throw new PermanentBackendException(e);
        } catch (final Exception e) {
            logger.error("Unable to clear storage from index due to general error.", e);
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtil.isString(dataType) &&
                !(mapping==Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType))) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Geoshape.class) {
            switch(mapping) {
                case DEFAULT:
                    return janusgraphPredicate == Geo.WITHIN || janusgraphPredicate == Geo.INTERSECT;
                case PREFIX_TREE:
                    return janusgraphPredicate == Geo.INTERSECT || janusgraphPredicate == Geo.WITHIN || janusgraphPredicate == Geo.CONTAINS;
            }
        } else if (AttributeUtil.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.CONTAINS_REGEX || janusgraphPredicate == Text.CONTAINS_FUZZY;
                case STRING:
                    return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL || janusgraphPredicate==Text.REGEX || janusgraphPredicate==Text.PREFIX  || janusgraphPredicate == Text.FUZZY;
//                case TEXTSTRING:
//                    return (janusgraphPredicate instanceof Text) || janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL;
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
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType == Instant.class || dataType == Boolean.class || dataType == UUID.class) {
            if (mapping==Mapping.DEFAULT) return true;
        } else if (AttributeUtil.isString(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.TEXT || mapping==Mapping.STRING) return true;
        } else if (AttributeUtil.isGeo(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.PREFIX_TREE) return true;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation keyInfo) {
        Preconditions.checkArgument(!StringUtils.containsAny(key, new char[]{' '}),"Invalid key name provided: %s",key);
        if (!dynFields) return key;
        if (ParameterType.MAPPED_NAME.hasParameter(keyInfo.getParameters())) return key;
        String postfix;
        final Class datatype = keyInfo.getDataType();
        if (AttributeUtil.isString(datatype)) {
            final Mapping map = getStringMapping(keyInfo);
            switch (map) {
                case TEXT: postfix = "_t"; break;
                case STRING: postfix = "_s"; break;
                default: throw new IllegalArgumentException("Unsupported string mapping: " + map);
            }
        } else if (AttributeUtil.isWholeNumber(datatype)) {
            if (datatype.equals(Long.class)) postfix = "_l";
            else postfix = "_i";
        } else if (AttributeUtil.isDecimal(datatype)) {
            if (datatype.equals(Float.class)) postfix = "_f";
            else postfix = "_d";
        } else if (datatype.equals(Geoshape.class)) {
            postfix = "_g";
        } else if (datatype.equals(Date.class) || datatype.equals(Instant.class)) {
            postfix = "_dt";
        } else if (datatype.equals(Boolean.class)) {
            postfix = "_b";
        } else if (datatype.equals(UUID.class)) {
            postfix = "_uuid";
        } else throw new IllegalArgumentException("Unsupported data type ["+datatype+"] for field: " + key);
        if (keyInfo.getCardinality() == Cardinality.SET || keyInfo.getCardinality() == Cardinality.LIST) {
                postfix += "s";
        }
        return key+postfix;
    }

    @Override
    public IndexFeatures getFeatures() {
        return SOLR_FEATURES;
    }

    @Override
    public boolean exists() throws BackendException {
        if (mode!=Mode.CLOUD) throw new UnsupportedOperationException("Operation only supported for SolrCloud");
        final CloudSolrClient server = (CloudSolrClient) solrClient;
        try {
            final ZkStateReader zkStateReader = server.getZkStateReader();
            zkStateReader.forciblyRefreshAllClusterStateSlow();
            final ClusterState clusterState = zkStateReader.getClusterState();
            final Map<String, DocCollection> collections = clusterState.getCollectionsMap();
            return collections != null && !collections.isEmpty();
        } catch (KeeperException | InterruptedException e) {
            throw new PermanentBackendException("Unable to check if index exists", e);
        }
    }

    /*
    ################# UTILITY METHODS #######################
     */

    private static Mapping getStringMapping(KeyInformation information) {
        assert AttributeUtil.isString(information.getDataType());
        Mapping map = Mapping.getMapping(information);
        if (map==Mapping.DEFAULT) map = Mapping.TEXT;
        return map;
    }

    private static Map<Geo, String> spatialPredicates() {
        return Collections.unmodifiableMap(Stream.of(
                new SimpleEntry<>(Geo.WITHIN, "IsWithin"),
                new SimpleEntry<>(Geo.CONTAINS, "Contains"),
                new SimpleEntry<>(Geo.INTERSECT, "Intersects"),
                new SimpleEntry<>(Geo.DISJOINT, "IsDisjointTo"))
                .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())));
    }

    private UpdateRequest newUpdateRequest() {
        final UpdateRequest req = new UpdateRequest();
        if(waitSearcher) {
            req.setAction(UpdateRequest.ACTION.COMMIT, true, true);
        }
        return req;
    }

    private BackendException storageException(Exception solrException) {
        return new TemporaryBackendException("Unable to complete query on Solr.", solrException);
    }

    private static void createCollectionIfNotExists(CloudSolrClient client, Configuration config, String collection)
            throws IOException, SolrServerException, KeeperException, InterruptedException {
        if (!checkIfCollectionExists(client, collection)) {
            final Integer numShards = config.get(NUM_SHARDS);
            final Integer maxShardsPerNode = config.get(MAX_SHARDS_PER_NODE);
            final Integer replicationFactor = config.get(REPLICATION_FACTOR);


            // Ideally this property used so a new configset is not uploaded for every single
            // index (collection) created in solr.
            // if a generic configSet is not set, make the configset name the same as the collection.
            // This was the default behavior before a default configSet could be specified
            final String  genericConfigSet = config.has(SOLR_DEFAULT_CONFIG) ? config.get(SOLR_DEFAULT_CONFIG):collection;

            final CollectionAdminRequest.Create createRequest = CollectionAdminRequest.createCollection(collection, genericConfigSet, numShards, replicationFactor);
            createRequest.setMaxShardsPerNode(maxShardsPerNode);

            final CollectionAdminResponse createResponse = createRequest.process(client);
            if (createResponse.isSuccess()) {
                logger.trace("Collection {} successfully created.", collection);
            } else {
                throw new SolrServerException(Joiner.on("\n").join(createResponse.getErrorMessages()));
            }
        }

        waitForRecoveriesToFinish(client, collection);
    }

    /**
     * Checks if the collection has already been created in Solr.
     */
    private static boolean checkIfCollectionExists(CloudSolrClient server, String collection) throws KeeperException, InterruptedException {
        final ZkStateReader zkStateReader = server.getZkStateReader();
        zkStateReader.forceUpdateCollection(collection);
        final ClusterState clusterState = zkStateReader.getClusterState();
        return clusterState.getCollectionOrNull(collection) != null;
    }

    /**
     * Wait for all the collection shards to be ready.
     */
    private static void waitForRecoveriesToFinish(CloudSolrClient server, String collection) throws KeeperException, InterruptedException {
        final ZkStateReader zkStateReader = server.getZkStateReader();
        try {
            boolean cont = true;

            while (cont) {
                boolean sawLiveRecovering = false;
                zkStateReader.forceUpdateCollection(collection);
                final ClusterState clusterState = zkStateReader.getClusterState();
                final Map<String, Slice> slices = clusterState.getCollection(collection).getSlicesMap();
                Preconditions.checkNotNull(slices, "Could not find collection:" + collection);

               // change paths for Replica.State per Solr refactoring
               // remove SYNC state per: http://tinyurl.com/pag6rwt
               for (final Map.Entry<String, Slice> entry : slices.entrySet()) {
                    final Map<String, Replica> shards = entry.getValue().getReplicasMap();
                    for (final Map.Entry<String, Replica> shard : shards.entrySet()) {
                        final String state = shard.getValue().getStr(ZkStateReader.STATE_PROP).toUpperCase();
                        if ((Replica.State.RECOVERING.name().equals(state) || Replica.State.DOWN.name().equals(state))
                                && clusterState.liveNodesContain(shard.getValue().getStr(
                                ZkStateReader.NODE_NAME_PROP))) {
                            sawLiveRecovering = true;
                        }
                    }
                }


                if (!sawLiveRecovering) {
                    cont = false;
                } else {
                    Thread.sleep(1000);
                }
            }
        } finally {
            logger.info("Exiting solr wait");
        }
    }

}
