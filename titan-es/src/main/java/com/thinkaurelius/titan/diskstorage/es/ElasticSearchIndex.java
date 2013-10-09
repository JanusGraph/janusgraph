package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticSearchInterruptedException;
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
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ElasticSearchIndex implements IndexProvider {

    private Logger log = LoggerFactory.getLogger(ElasticSearchIndex.class);

    private static final String[] DATA_SUBDIRS = {"data", "work", "logs"};

    public static final String MAX_RESULT_SET_SIZE_KEY = "max-result-set-size";
    public static final int MAX_RESULT_SET_SIZE_DEFAULT = 100000;

    public static final String CLIENT_ONLY_KEY = "client-only";
    public static final boolean CLIENT_ONLY_DEFAULT = true;
    public static final String CLUSTER_NAME_KEY = "cluster-name";
    public static final String CLUSTER_NAME_DEFAULT = "elasticsearch";
    public static final String INDEX_NAME_KEY = "index-name";
    public static final String INDEX_NAME_DEFAULT = "titan";
    public static final String LOCAL_MODE_KEY = "local-mode";
    public static final boolean LOCAL_MODE_DEFAULT = false;
    public static final String CLIENT_SNIFF_KEY = "sniff";
    public static final boolean CLIENT_SNIFF_DEFAULT = true;

    //    public static final String HOST_NAMES_KEY = "hosts";
    public static final int HOST_PORT_DEFAULT = 9300;

    public static final String ES_YML_KEY = "config-file";


    private final Node node;
    private final Client client;
    private final String indexName;
    private final int maxResultsSize;

    public ElasticSearchIndex(Configuration config) {
        indexName = config.getString(INDEX_NAME_KEY, INDEX_NAME_DEFAULT);

        checkExpectedClientVersion();

        if (!config.containsKey(GraphDatabaseConfiguration.HOSTNAME_KEY)) {
            boolean clientOnly = config.getBoolean(CLIENT_ONLY_KEY, CLIENT_ONLY_DEFAULT);
            boolean local = config.getBoolean(LOCAL_MODE_KEY, LOCAL_MODE_DEFAULT);

            NodeBuilder builder = NodeBuilder.nodeBuilder();
            Preconditions.checkArgument(config.containsKey(ES_YML_KEY) || config.containsKey(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY),
                    "Must either configure configuration file or base directory");
            if (config.containsKey(ES_YML_KEY)) {
                String configFile = config.getString(ES_YML_KEY);
                log.debug("Configuring ES from YML file [{}]", configFile);
                Settings settings = ImmutableSettings.settingsBuilder().loadFromSource(configFile).build();
                builder.settings(settings);
            } else {
                String dataDirectory = config.getString(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY);
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
                builder.settings(b.build());

                String clustername = config.getString(CLUSTER_NAME_KEY, CLUSTER_NAME_DEFAULT);
                Preconditions.checkArgument(StringUtils.isNotBlank(clustername), "Invalid cluster name: %s", clustername);
                builder.clusterName(clustername);
            }

            node = builder.client(clientOnly).data(!clientOnly).local(local).node();
            client = node.client();

        } else {
            ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
            if (config.containsKey(CLUSTER_NAME_KEY)) {
                String clustername = config.getString(CLUSTER_NAME_KEY, CLUSTER_NAME_DEFAULT);
                Preconditions.checkArgument(StringUtils.isNotBlank(clustername), "Invalid cluster name: %s", clustername);
                settings.put("cluster.name", clustername);
            } else {
                settings.put("client.transport.ignore_cluster_name", true);
            }
            log.debug("Transport sniffing enabled: {}", config.getBoolean(CLIENT_SNIFF_KEY, CLIENT_SNIFF_DEFAULT));
            settings.put("client.transport.sniff", config.getBoolean(CLIENT_SNIFF_KEY, CLIENT_SNIFF_DEFAULT));
            TransportClient tc = new TransportClient(settings.build());
            for (String host : config.getStringArray(GraphDatabaseConfiguration.HOSTNAME_KEY)) {
                String[] hostparts = host.split(":");
                String hostname = hostparts[0];
                int hostport = HOST_PORT_DEFAULT;
                if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
                log.info("Configured remote host: {} : {}", hostname, hostport);
                tc.addTransportAddress(new InetSocketTransportAddress(hostname, hostport));
            }
            client = tc;
            node = null;
        }

        maxResultsSize = config.getInt(MAX_RESULT_SET_SIZE_KEY, MAX_RESULT_SET_SIZE_DEFAULT);
        log.debug("Configured ES query result set max size to {}", maxResultsSize);

        client.admin().cluster().prepareHealth()
                .setWaitForYellowStatus().execute().actionGet();

        //Create index if it does not already exist
        IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
        if (!response.isExists()) {
            CreateIndexResponse create = client.admin().indices().prepareCreate(indexName).execute().actionGet();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new TitanException("Interrupted while waiting for index to settle in", e);
            }
            if (!create.isAcknowledged()) throw new IllegalArgumentException("Could not create index: " + indexName);
        }
    }

    private StorageException convert(Exception esException) {
        if (esException instanceof ElasticSearchInterruptedException) {
            return new TemporaryStorageException("Interrupted while waiting for response", esException);
        } else {
            return new PermanentStorageException("Unknown exception while executing index operation", esException);
        }
    }

    @Override
    public void register(String store, String key, Class<?> dataType, TransactionHandle tx) throws StorageException {
        XContentBuilder mapping = null;

        try {
            mapping = XContentFactory.jsonBuilder().
                    startObject().
                    startObject(store).
                    startObject("properties").
                    startObject(key);

            if (AttributeUtil.isString(dataType)) {
                log.debug("Registering string type for {}", key);
                mapping.field("type", "string");
            } else if (dataType == Float.class || dataType == FullFloat.class) {
                log.debug("Registering float type for {}", key);
                mapping.field("type", "float");
            } else if (dataType == Double.class || dataType == FullDouble.class) {
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
            }

            mapping.endObject().endObject().endObject().endObject();

        } catch (IOException e) {
            throw new PermanentStorageException("Could not render json for put mapping request", e);
        }

        try {
            PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).
                    setIgnoreConflicts(false).setType(store).setSource(mapping).execute().actionGet();
        } catch (Exception e) {
            throw convert(e);
        }
//        if (dataType == Geoshape.class) { //Only need to update for geoshape
//            log.debug("Registering geo_point type for {}", key);
//            XContentBuilder mapping = null;
//            try {
//                mapping =
//                        XContentFactory.jsonBuilder()
//                                .startObject()
//                                .startObject(store)
//                                .startObject("properties")
//                                .startObject(key)
//                                .field("type", "geo_point")
//                                .endObject()
//                                .endObject()
//                                .endObject()
//                                .endObject();
//            } catch (IOException e) {
//                throw new PermanentStorageException("Could not render json for put mapping request", e);
//            }
//            try {
//                PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).
//                        setIgnoreConflicts(false).setType(store).setSource(mapping).execute().actionGet();
//            } catch (Exception e) {
//                throw convert(e);
//            }
//        }
    }

    public XContentBuilder getContent(List<IndexEntry> additions) throws StorageException {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (IndexEntry add : additions) {
                if (add.value instanceof Number) {
                    if (AttributeUtil.isWholeNumber((Number) add.value)) {
                        builder.field(add.key, ((Number) add.value).longValue());
                    } else { //double or float
                        builder.field(add.key, ((Number) add.value).doubleValue());
                    }
                } else if (AttributeUtil.isString(add.value)) {
                    builder.field(add.key, (String) add.value);
                } else if (add.value instanceof Geoshape) {
                    Geoshape shape = (Geoshape) add.value;
                    if (shape.getType() == Geoshape.Type.POINT) {
                        Geoshape.Point p = shape.getPoint();
                        builder.field(add.key, new double[]{p.getLongitude(), p.getLatitude()});
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
                } else throw new IllegalArgumentException("Unsupported type: " + add.value);

            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new PermanentStorageException("Could not write json");
        }
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, TransactionHandle tx) throws StorageException {
        BulkRequestBuilder brb = client.prepareBulk();
        int bulkrequests = 0;
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String storename = stores.getKey();
                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    String docid = entry.getKey();
                    IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());

                    //Deletions first
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            log.trace("Deleting entire document {}", docid);
                            brb.add(new DeleteRequest(indexName, storename, docid));
                            bulkrequests++;
                        } else {
                            Set<String> deletions = Sets.newHashSet(mutation.getDeletions());
                            if (mutation.hasAdditions()) {
                                for (IndexEntry ie : mutation.getAdditions()) {
                                    deletions.remove(ie.key);
                                }
                            }
                            if (!deletions.isEmpty()) {
                                //TODO make part of batch mutation if/when possible
                                StringBuilder script = new StringBuilder();
                                for (String key : deletions) {
                                    script.append("ctx._source.remove(\"" + key + "\"); ");
                                }
                                log.trace("Deleting individual fields [{}] for document {}", deletions, docid);
                                client.prepareUpdate(indexName, storename, docid).setScript(script.toString()).execute().actionGet();
                            }
                        }
                    }

                    if (mutation.hasAdditions()) {
                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", docid);
                            brb.add(new IndexRequest(indexName, storename, docid).source(getContent(mutation.getAdditions())));
                            bulkrequests++;
                        } else { //Update: TODO make part of batch mutation if/when possible
                            boolean needUpsert = !mutation.hasDeletions();
                            XContentBuilder builder = getContent(mutation.getAdditions());
                            UpdateRequestBuilder update = client.prepareUpdate(indexName, storename, docid).setDoc(builder);
                            if (needUpsert) update.setUpsert(builder);
                            log.trace("Updating document {} with upsert {}", docid, needUpsert);
                            update.execute().actionGet();
                        }
                    }

                }
            }
            if (bulkrequests > 0) brb.execute().actionGet();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public FilterBuilder getFilter(Condition<?> condition) {
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
                if (titanPredicate == Text.CONTAINS) {
                    return FilterBuilders.termFilter(key, ((String) value).toLowerCase());
                } else if (titanPredicate == Text.PREFIX) {
                    return FilterBuilders.prefixFilter(key, ((String) value).toLowerCase());
                } else if (titanPredicate == Text.REGEX) {
                    return FilterBuilders.regexpFilter(key, (String) value);
//                } else if (relation == Cmp.EQUAL) {
//                    return new TermsFilter(new Term(key+STR_SUFFIX,(String)value));
//                } else if (relation == Cmp.NOT_EQUAL) {
//                    BooleanFilter q = new BooleanFilter();
//                    q.add(new TermsFilter(new Term(key+STR_SUFFIX,(String)value)), BooleanClause.Occur.MUST_NOT);
//                    return q;
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
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        } else if (condition instanceof Not) {
            return FilterBuilders.notFilter(getFilter(((Not) condition).getChild()));
        } else if (condition instanceof And) {
            AndFilterBuilder b = FilterBuilders.andFilter();
            for (Condition c : condition.getChildren()) {
                b.add(getFilter(c));
            }
            return b;
        } else if (condition instanceof Or) {
            OrFilterBuilder b = FilterBuilders.orFilter();
            for (Condition c : condition.getChildren()) {
                b.add(getFilter(c));
            }
            return b;
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
    }

    @Override
    public List<String> query(IndexQuery query, TransactionHandle tx) throws StorageException {
        SearchRequestBuilder srb = client.prepareSearch(indexName);
        srb.setTypes(query.getStore());
        srb.setQuery(QueryBuilders.matchAllQuery());
        srb.setFilter(getFilter(query.getCondition()));
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
    public boolean supports(Class<?> dataType, TitanPredicate titanPredicate) {
        if (Number.class.isAssignableFrom(dataType)) {
            if (titanPredicate instanceof Cmp) return true;
            else return false;
        } else if (dataType == Geoshape.class) {
            return titanPredicate == Geo.WITHIN;
        } else if (dataType == String.class) {
            return titanPredicate == Text.CONTAINS || titanPredicate == Text.PREFIX || titanPredicate == Text.REGEX;
        } else return false;
    }

    @Override
    public boolean supports(Class<?> dataType) {
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class || AttributeUtil.isString(dataType))
            return true;
        else return false;
    }

    @Override
    public TransactionHandle beginTransaction() throws StorageException {
        return TransactionHandle.NO_TRANSACTION;
    }

    @Override
    public void close() throws StorageException {
        client.close();
        if (node != null && !node.isClosed()) {
            node.close();
        }
    }

    @Override
    public void clearStorage() throws StorageException {
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
            throw new PermanentStorageException("Could not delete index " + indexName, e);
        } finally {
            close();
        }
    }

    private void checkExpectedClientVersion() {
        if (!Version.CURRENT.equals(ElasticSearchConstants.ES_VERSION_EXPECTED)) {
            log.warn("ES client version {} does not match the version with which Titan was compiled {}.  This might cause problems.",
                    Version.CURRENT, ElasticSearchConstants.ES_VERSION_EXPECTED);
        } else {
            log.debug("Found expected ES client version: {} (OK)", Version.CURRENT);
        }
    }
}
