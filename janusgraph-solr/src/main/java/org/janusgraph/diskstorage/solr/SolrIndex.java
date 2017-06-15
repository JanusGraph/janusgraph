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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.janusgraph.core.Cardinality;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.*;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.diskstorage.solr.transform.GeoToWktConverter;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.*;

import org.janusgraph.graphdb.types.ParameterType;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;

import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

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
            for (Mode m : Mode.values()) {
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


    private static final IndexFeatures SOLR_FEATURES = new IndexFeatures.Builder().supportsDocumentTTL()
            .setDefaultStringMapping(Mapping.TEXT).supportedStringMappings(Mapping.TEXT, Mapping.STRING).supportsCardinality(Cardinality.SINGLE).build();

    private final SolrClient solrClient;
    private final Configuration configuration;
    private final Mode mode;
    private final boolean dynFields;
    private final Map<String, String> keyFieldIds;
    private final String ttlField;
    private final int maxResults;
    private final boolean waitSearcher;

    public SolrIndex(final Configuration config) throws BackendException {
        Preconditions.checkArgument(config!=null);
        configuration = config;

        mode = Mode.parse(config.get(SOLR_MODE));
        dynFields = config.get(DYNAMIC_FIELDS);
        keyFieldIds = parseKeyFieldsForCollections(config);
        maxResults = config.get(INDEX_MAX_RESULT_SET_SIZE);
        ttlField = config.get(TTL_FIELD);
        waitSearcher = config.get(WAIT_SEARCHER);

        if (mode==Mode.CLOUD) {
            String zookeeperUrl = config.get(SolrIndex.ZOOKEEPER_URL);
            CloudSolrClient cloudServer = new CloudSolrClient(zookeeperUrl, true);
            cloudServer.connect();
            solrClient = cloudServer;
        } else if (mode==Mode.HTTP) {
            HttpClient clientParams = HttpClientUtil.createClient(new ModifiableSolrParams() {{
                add(HttpClientUtil.PROP_ALLOW_COMPRESSION, config.get(HTTP_ALLOW_COMPRESSION).toString());
                add(HttpClientUtil.PROP_CONNECTION_TIMEOUT, config.get(HTTP_CONNECTION_TIMEOUT).toString());
                add(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, config.get(HTTP_MAX_CONNECTIONS_PER_HOST).toString());
                add(HttpClientUtil.PROP_MAX_CONNECTIONS, config.get(HTTP_GLOBAL_MAX_CONNECTIONS).toString());
            }});

            solrClient = new LBHttpSolrClient(clientParams, config.get(HTTP_URLS));


        } else {
            throw new IllegalArgumentException("Unsupported Solr operation mode: " + mode);
        }
    }

    private Map<String, String> parseKeyFieldsForCollections(Configuration config) throws BackendException {
        Map<String, String> keyFieldNames = new HashMap<String, String>();
        String[] collectionFieldStatements = config.has(KEY_FIELD_NAMES)?config.get(KEY_FIELD_NAMES):new String[0];
        for (String collectionFieldStatement : collectionFieldStatements) {
            String[] parts = collectionFieldStatement.trim().split("=");
            if (parts.length != 2) {
                throw new PermanentBackendException("Unable to parse the collection name / key field name pair. It should be of the format collection=field");
            }
            String collectionName = parts[0];
            String keyFieldName = parts[1];
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
    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        if (mode==Mode.CLOUD) {
            CloudSolrClient client = (CloudSolrClient) solrClient;
            try {
                createCollectionIfNotExists(client, configuration, store);
            } catch (IOException e) {
                throw new PermanentBackendException(e);
            } catch (SolrServerException e) {
                throw new PermanentBackendException(e);
            } catch (InterruptedException e) {
                throw new PermanentBackendException(e);
            } catch (KeeperException e) {
                throw new PermanentBackendException(e);
            }
        }
        //Since all data types must be defined in the schema.xml, pre-registering a type does not work
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        logger.debug("Mutating SOLR");
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String collectionName = stores.getKey();
                String keyIdField = getKeyFieldId(collectionName);

                List<String> deleteIds = new ArrayList<String>();
                Collection<SolrInputDocument> changes = new ArrayList<SolrInputDocument>();

                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    String docId = entry.getKey();
                    IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());

                    //Handle any deletions
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            logger.trace("Deleting entire document {}", docId);
                            deleteIds.add(docId);
                        } else {
                            HashSet<IndexEntry> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                            if (mutation.hasAdditions()) {
                                for (IndexEntry indexEntry : mutation.getAdditions()) {
                                    fieldDeletions.remove(indexEntry);
                                }
                            }
                            deleteIndividualFieldsFromIndex(collectionName, keyIdField, docId, fieldDeletions);
                        }
                    }

                    if (mutation.hasAdditions()) {
                        int ttl = mutation.determineTTL();

                        SolrInputDocument doc = new SolrInputDocument();
                        doc.setField(keyIdField, docId);

                        boolean isNewDoc = mutation.isNew();

                        if (isNewDoc)
                            logger.trace("Adding new document {}", docId);

                        for (IndexEntry e : mutation.getAdditions()) {
                            final Object fieldValue = convertValue(e.value);
                            doc.setField(e.field, isNewDoc
                                    ? fieldValue : new HashMap<String, Object>(1) {{
                                put("set", fieldValue);
                            }});
                        }
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
        } catch (IllegalArgumentException e) {
            throw new PermanentBackendException("Unable to complete query on Solr.", e);
        } catch (Exception e) {
            throw storageException(e);
        }
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
            for (Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final String collectionName = stores.getKey();

                List<String> deleteIds = new ArrayList<String>();
                List<SolrInputDocument> newDocuments = new ArrayList<SolrInputDocument>();

                for (Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();

                    if (content == null || content.isEmpty()) {
                        if (logger.isTraceEnabled())
                            logger.trace("Deleting document [{}]", docID);

                        deleteIds.add(docID);
                        continue;
                    }

                    newDocuments.add(new SolrInputDocument() {{
                        setField(getKeyFieldId(collectionName), docID);

                        for (IndexEntry addition : content) {
                            Object fieldValue = addition.value;
                            setField(addition.field, convertValue(fieldValue));
                        }
                    }});
                }

                commitDeletes(collectionName, deleteIds);
                commitDocumentChanges(collectionName, newDocuments);
            }
        } catch (Exception e) {
            throw new TemporaryBackendException("Could not restore Solr index", e);
        }
    }

    private void deleteIndividualFieldsFromIndex(String collectionName, String keyIdField, String docId, HashSet<IndexEntry> fieldDeletions) throws SolrServerException, IOException {
        if (fieldDeletions.isEmpty()) return;

        Map<String, String> fieldDeletes = new HashMap<String, String>(1) {{ put("set", null); }};

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(keyIdField, docId);
        StringBuilder sb = new StringBuilder();
        for (IndexEntry fieldToDelete : fieldDeletions) {
            doc.addField(fieldToDelete.field, fieldDeletes);
            sb.append(fieldToDelete).append(",");
        }

        if (logger.isTraceEnabled())
            logger.trace("Deleting individual fields [{}] for document {}", sb.toString(), docId);

        UpdateRequest singleDocument = newUpdateRequest();
        singleDocument.add(doc);
        solrClient.request(singleDocument, collectionName);
    }

    private void commitDocumentChanges(String collectionName, Collection<SolrInputDocument> documents) throws SolrServerException, IOException {
        if (documents.size() == 0) return;

        try {
            solrClient.request(newUpdateRequest().add(documents), collectionName);
        } catch (HttpSolrClient.RemoteSolrException rse) {
            logger.error("Unable to save documents to Solr as one of the shape objects stored were not compatible with Solr.", rse);
            logger.error("Details in failed document batch: ");
            for (SolrInputDocument d : documents) {
                Collection<String> fieldNames = d.getFieldNames();
                for (String name : fieldNames) {
                    logger.error(name + ":" + d.getFieldValue(name).toString());
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
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        List<String> result;
        String collection = query.getStore();
        String keyIdField = getKeyFieldId(collection);
        SolrQuery solrQuery = new SolrQuery("*:*");
        String queryFilter = buildQueryFilter(query.getCondition(), informations.get(collection));
        solrQuery.addFilterQuery(queryFilter);
        if (!query.getOrder().isEmpty()) {
            List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (IndexQuery.OrderEntry order1 : orders) {
                String item = order1.getKey();
                SolrQuery.ORDER order = order1.getOrder() == Order.ASC ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
                solrQuery.addSort(new SolrQuery.SortClause(item, order));
            }
        }
        solrQuery.setStart(0);
        if (query.hasLimit()) {
            solrQuery.setRows(query.getLimit());
        } else {
            solrQuery.setRows(maxResults);
        }
        try {
            QueryResponse response = solrClient.query(collection, solrQuery);

            if (logger.isDebugEnabled())
                logger.debug("Executed query [{}] in {} ms", query.getCondition(), response.getElapsedTime());

            int totalHits = response.getResults().size();

            if (!query.hasLimit() && totalHits >= maxResults)
                logger.warn("Query result set truncated to first [{}] elements for query: {}", maxResults, query);

            result = new ArrayList<String>(totalHits);
            for (SolrDocument hit : response.getResults()) {
                result.add(hit.getFieldValue(keyIdField).toString());
            }
        } catch (IOException e) {
            logger.error("Query did not complete : ", e);
            throw new PermanentBackendException(e);
        } catch (SolrServerException e) {
            logger.error("Unable to query Solr index.", e);
            throw new PermanentBackendException(e);
        }
        return result;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        List<RawQuery.Result<String>> result;
        String collection = query.getStore();
        String keyIdField = getKeyFieldId(collection);
        SolrQuery solrQuery = new SolrQuery(query.getQuery())
                                .addField(keyIdField)
                                .setIncludeScore(true)
                                .setStart(query.getOffset())
                                .setRows(query.hasLimit() ? query.getLimit() : maxResults);

        try {
            QueryResponse response = solrClient.query(collection, solrQuery);
            if (logger.isDebugEnabled())
                logger.debug("Executed query [{}] in {} ms", query.getQuery(), response.getElapsedTime());

            int totalHits = response.getResults().size();
            if (!query.hasLimit() && totalHits >= maxResults) {
                logger.warn("Query result set truncated to first [{}] elements for query: {}", maxResults, query);
            }
            result = new ArrayList<RawQuery.Result<String>>(totalHits);

            for (SolrDocument hit : response.getResults()) {
                double score = Double.parseDouble(hit.getFieldValue("score").toString());
                result.add(new RawQuery.Result<String>(hit.getFieldValue(keyIdField).toString(), score));
            }
        } catch (IOException e) {
            logger.error("Query did not complete : ", e);
            throw new PermanentBackendException(e);
        } catch (SolrServerException e) {
            logger.error("Unable to query Solr index.", e);
            throw new PermanentBackendException(e);
        }
        return result;
    }

    private static String escapeValue(Object value) {
        return ClientUtils.escapeQueryChars(value.toString());
    }

    public String buildQueryFilter(Condition<JanusGraphElement> condition, KeyInformation.StoreRetriever informations) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<String, JanusGraphElement> atom = (PredicateCondition<String, JanusGraphElement>) condition;
            Object value = atom.getValue();
            String key = atom.getKey();
            JanusGraphPredicate janusgraphPredicate = atom.getPredicate();

            if (value instanceof Number) {
                String queryValue = escapeValue(value);
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on numeric types: " + janusgraphPredicate);
                Cmp numRel = (Cmp) janusgraphPredicate;
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
                Mapping map = getStringMapping(informations.get(key));
                assert map==Mapping.TEXT || map==Mapping.STRING;
                if (map==Mapping.TEXT && !Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + janusgraphPredicate);
                if (map==Mapping.STRING && Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + janusgraphPredicate);

                //Special case
                if (janusgraphPredicate == Text.CONTAINS) {
                    //e.g. - if terms tomorrow and world were supplied, and fq=text:(tomorrow  world)
                    //sample data set would return 2 documents: one where text = Tomorrow is the World,
                    //and the second where text = Hello World. Hence, we are decomposing the query string
                    //and building an AND query explicitly because we need AND semantics
                    value = ((String) value).toLowerCase();
                    List<String> terms = Text.tokenize((String) value);

                    if (terms.isEmpty()) {
                        return "";
                    } else if (terms.size() == 1) {
                        return (key + ":(" + escapeValue(terms.get(0)) + ")");
                    } else {
                        And<JanusGraphElement> andTerms = new And<JanusGraphElement>();
                        for (String term : terms) {
                            andTerms.add(new PredicateCondition<String, JanusGraphElement>(key, janusgraphPredicate, term));
                        }
                        return buildQueryFilter(andTerms, informations);
                    }
                }
                if (janusgraphPredicate == Text.PREFIX || janusgraphPredicate == Text.CONTAINS_PREFIX) {
                    return (key + ":" + escapeValue(value) + "*");
                } else if (janusgraphPredicate == Text.REGEX || janusgraphPredicate == Text.CONTAINS_REGEX) {
                    return (key + ":/" + value + "/");
                } else if (janusgraphPredicate == Cmp.EQUAL) {
                    return (key + ":\"" + escapeValue(value) + "\"");
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    return ("-" + key + ":\"" + escapeValue(value) + "\"");
                } else {
                    throw new IllegalArgumentException("Relation is not supported for string value: " + janusgraphPredicate);
                }
            } else if (value instanceof Geoshape) {
                Geoshape geo = (Geoshape)value;
                if (geo.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = geo.getPoint();
                    return ("{!geofilt sfield=" + key +
                            " pt=" + center.getLatitude() + "," + center.getLongitude() +
                            " d=" + geo.getRadius() + "} distErrPct=0"); //distance in kilometers
                } else if (geo.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = geo.getPoint(0);
                    Geoshape.Point northeast = geo.getPoint(1);
                    return (key + ":[" + southwest.getLatitude() + "," + southwest.getLongitude() +
                            " TO " + northeast.getLatitude() + "," + northeast.getLongitude() + "]");
                } else if (geo.getType() == Geoshape.Type.POLYGON) {
                    List<Geoshape.Point> coordinates = getPolygonPoints(geo);
                    StringBuilder poly = new StringBuilder(key + ":\"IsWithin(POLYGON((");
                    for (Geoshape.Point coordinate : coordinates) {
                        poly.append(coordinate.getLongitude()).append(" ").append(coordinate.getLatitude()).append(", ");
                    }
                    //close the polygon with the first coordinate
                    poly.append(coordinates.get(0).getLongitude()).append(" ").append(coordinates.get(0).getLatitude());
                    poly.append(")))\" distErrPct=0");
                    return (poly.toString());
                }
            } else if (value instanceof Date || value instanceof Instant) {
                String s = value.toString();
                String queryValue = escapeValue(value instanceof Date ? toIsoDate((Date) value) : value.toString());
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on date types: " + janusgraphPredicate);
                Cmp numRel = (Cmp) janusgraphPredicate;

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
                Cmp numRel = (Cmp) janusgraphPredicate;
                String queryValue = escapeValue(value);
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
            String sub = buildQueryFilter(((Not)condition).getChild(),informations);
            if (StringUtils.isNotBlank(sub)) return "-("+sub+")";
            else return "";
        } else if (condition instanceof And) {
            int numChildren = ((And) condition).size();
            StringBuilder sb = new StringBuilder();
            for (Condition<JanusGraphElement> c : condition.getChildren()) {
                String sub = buildQueryFilter(c, informations);

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
            StringBuilder sb = new StringBuilder();
            int element=0;
            for (Condition<JanusGraphElement> c : condition.getChildren()) {
                String sub = buildQueryFilter(c,informations);
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
        return null;
    }

    private String toIsoDate(Date value) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(tz);
        return df.format(value);
    }

    private List<Geoshape.Point> getPolygonPoints(Geoshape polygon) {
        List<Geoshape.Point> locations = new ArrayList<Geoshape.Point>();

        int index = 0;
        boolean hasCoordinates = true;
        while (hasCoordinates) {
            try {
                locations.add(polygon.getPoint(index));
            } catch (ArrayIndexOutOfBoundsException ignore) {
                //just means we asked for a point past the size of the list
                //of known coordinates
                hasCoordinates = false;
            }
        }

        return locations;
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
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            if (mode!=Mode.CLOUD) throw new UnsupportedOperationException("Operation only supported for SolrCloud");
            logger.debug("Clearing storage from Solr: {}", solrClient);
            ZkStateReader zkStateReader = ((CloudSolrClient) solrClient).getZkStateReader();
            zkStateReader.updateClusterState(true);
            ClusterState clusterState = zkStateReader.getClusterState();
            for (String collection : clusterState.getCollections()) {
                logger.debug("Clearing collection [{}] in Solr",collection);
                UpdateRequest deleteAll = newUpdateRequest();
                deleteAll.deleteByQuery("*:*");
                solrClient.request(deleteAll, collection);
            }

        } catch (SolrServerException e) {
            logger.error("Unable to clear storage from index due to server error on Solr.", e);
            throw new PermanentBackendException(e);
        } catch (IOException e) {
            logger.error("Unable to clear storage from index due to low-level I/O error.", e);
            throw new PermanentBackendException(e);
        } catch (Exception e) {
            logger.error("Unable to clear storage from index due to general error.", e);
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtil.isString(dataType)) return false;

        if(information.getCardinality() != Cardinality.SINGLE) {
            return false;
        }

        if (Number.class.isAssignableFrom(dataType)) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Geoshape.class) {
            return janusgraphPredicate == Geo.WITHIN;
        } else if (AttributeUtil.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.CONTAINS_REGEX;
                case STRING:
                    return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL || janusgraphPredicate==Text.REGEX || janusgraphPredicate==Text.PREFIX;
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
        if(information.getCardinality() != Cardinality.SINGLE) {
            return false;
        }
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class || dataType == Date.class || dataType == Instant.class || dataType == Boolean.class || dataType == UUID.class) {
            if (mapping==Mapping.DEFAULT) return true;
        } else if (AttributeUtil.isString(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.TEXT || mapping==Mapping.STRING) return true;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation keyInfo) {
        Preconditions.checkArgument(!StringUtils.containsAny(key, new char[]{' '}),"Invalid key name provided: %s",key);
        if (!dynFields) return key;
        if (ParameterType.MAPPED_NAME.hasParameter(keyInfo.getParameters())) return key;
        String postfix;
        Class datatype = keyInfo.getDataType();
        if (AttributeUtil.isString(datatype)) {
            Mapping map = getStringMapping(keyInfo);
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
        return key+postfix;
    }

    @Override
    public IndexFeatures getFeatures() {
        return SOLR_FEATURES;
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

    private UpdateRequest newUpdateRequest() {
        UpdateRequest req = new UpdateRequest();
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
            Integer numShards = config.get(NUM_SHARDS);
            Integer maxShardsPerNode = config.get(MAX_SHARDS_PER_NODE);
            Integer replicationFactor = config.get(REPLICATION_FACTOR);


            // Ideally this property used so a new configset is not uploaded for every single
            // index (collection) created in solr.
            // if a generic configSet is not set, make the configset name the same as the collection.
            // This was the default behavior before a default configSet could be specified 
            String  genericConfigSet = config.has(SOLR_DEFAULT_CONFIG) ? config.get(SOLR_DEFAULT_CONFIG):collection;

            CollectionAdminRequest.Create createRequest = new CollectionAdminRequest.Create();

            createRequest.setConfigName(genericConfigSet);
            createRequest.setCollectionName(collection);
            createRequest.setNumShards(numShards);
            createRequest.setMaxShardsPerNode(maxShardsPerNode);
            createRequest.setReplicationFactor(replicationFactor);

            CollectionAdminResponse createResponse = createRequest.process(client);
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
        ZkStateReader zkStateReader = server.getZkStateReader();
        zkStateReader.updateClusterState(true);
        ClusterState clusterState = zkStateReader.getClusterState();
        return clusterState.getCollectionOrNull(collection) != null;
    }

    /**
     * Wait for all the collection shards to be ready.
     */
    private static void waitForRecoveriesToFinish(CloudSolrClient server, String collection) throws KeeperException, InterruptedException {
        ZkStateReader zkStateReader = server.getZkStateReader();
        try {
            boolean cont = true;

            while (cont) {
                boolean sawLiveRecovering = false;
                zkStateReader.updateClusterState(true);
                ClusterState clusterState = zkStateReader.getClusterState();
                Map<String, Slice> slices = clusterState.getSlicesMap(collection);
                Preconditions.checkNotNull("Could not find collection:" + collection, slices);

               // change paths for Replica.State per Solr refactoring
               // remove SYNC state per: http://tinyurl.com/pag6rwt
               for (Map.Entry<String, Slice> entry : slices.entrySet()) {
                    Map<String, Replica> shards = entry.getValue().getReplicasMap();
                    for (Map.Entry<String, Replica> shard : shards.entrySet()) {
                        String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
                        if ((state.equals(Replica.State.RECOVERING) || state.equals(Replica.State.DOWN))
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
