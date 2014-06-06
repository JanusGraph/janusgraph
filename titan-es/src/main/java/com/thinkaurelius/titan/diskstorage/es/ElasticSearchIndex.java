package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.util.DefaultTransaction;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;

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
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@PreInitializeConfigOptions
public class ElasticSearchIndex implements IndexProvider {

    private Logger log = LoggerFactory.getLogger(ElasticSearchIndex.class);

    private static final String[] DATA_SUBDIRS = {"data", "work", "logs"};

    public static final ConfigOption<Integer> MAX_RESULT_SET_SIZE = new ConfigOption<Integer>(INDEX_NS,"max-result-set-size",
            "Maxium number of results to return if no limit is specified",
            ConfigOption.Type.MASKABLE, 100000);
    public static final ConfigOption<Boolean> CLIENT_ONLY = new ConfigOption<Boolean>(INDEX_NS,"client-only",
            "Whether Titan connects to the indexing backend as a client",
            ConfigOption.Type.GLOBAL_OFFLINE, true);
    public static final ConfigOption<String> CLUSTER_NAME = new ConfigOption<String>(INDEX_NS,"cluster-name",
            "The name of the indexing backend cluster",
            ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");
    public static final ConfigOption<Boolean> LOCAL_MODE = new ConfigOption<Boolean>(INDEX_NS,"local-mode",
            "Whether a full indexing instances is started embedded",
            ConfigOption.Type.GLOBAL_OFFLINE, false);
    public static final ConfigOption<Boolean> CLIENT_SNIFF = new ConfigOption<Boolean>(INDEX_NS,"sniff",
            "Whether to enable cluster sniffing",
            ConfigOption.Type.MASKABLE, true);

//
//    public static final String MAX_RESULT_SET_SIZE_KEY = "max-result-set-size";
//    public static final int MAX_RESULT_SET_SIZE_DEFAULT = 100000;

//    public static final String CLIENT_ONLY_KEY = "client-only";
//    public static final boolean CLIENT_ONLY_DEFAULT = true;
//    public static final String CLUSTER_NAME_KEY = "cluster-name";
//    public static final String CLUSTER_NAME_DEFAULT = "elasticsearch";
//    public static final String INDEX_NAME_KEY = "index-name";
//    public static final String INDEX_NAME_DEFAULT = "titan";

//    public static final String LOCAL_MODE_KEY = "local-mode";
//    public static final boolean LOCAL_MODE_DEFAULT = false;
//    public static final String CLIENT_SNIFF_KEY = "sniff";
//    public static final boolean CLIENT_SNIFF_DEFAULT = true;

    //    public static final String HOST_NAMES_KEY = "hosts";
    public static final int HOST_PORT_DEFAULT = 9300;

//    public static final String ES_YML_KEY = "config-file";


    private final Node node;
    private final Client client;
    private final String indexName;
    private final int maxResultsSize;

    public ElasticSearchIndex(Configuration config) {
        indexName = config.get(INDEX_NAME);

        checkExpectedClientVersion();

        if (config.get(LOCAL_MODE)) {

            log.debug("Configuring ES for JVM local transport");

            boolean clientOnly = config.get(CLIENT_ONLY);
            boolean local = config.get(LOCAL_MODE);

            NodeBuilder builder = NodeBuilder.nodeBuilder();
            Preconditions.checkArgument(config.has(INDEX_CONF_FILE) || config.has(INDEX_DIRECTORY),
                    "Must either configure configuration file or base directory");
            if (config.has(INDEX_CONF_FILE)) {
                String configFile = config.get(INDEX_CONF_FILE);
                log.debug("Configuring ES from YML file [{}]", configFile);
                Settings settings = ImmutableSettings.settingsBuilder().loadFromSource(configFile).build();
                builder.settings(settings);
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

        maxResultsSize = config.get(MAX_RESULT_SET_SIZE);
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
        if (esException instanceof InterruptedException) {
            return new TemporaryStorageException("Interrupted while waiting for response", esException);
        } else {
            return new PermanentStorageException("Unknown exception while executing index operation", esException);
        }
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws StorageException {
        XContentBuilder mapping = null;
        Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map==Mapping.DEFAULT || AttributeUtil.isString(dataType),
                "Specified illegal mapping [%s] for data type [%s]",map,dataType);

        try {
            mapping = XContentFactory.jsonBuilder().
                    startObject().
                    startObject(store).
                    startObject("properties").
                    startObject(key);

            if (AttributeUtil.isString(dataType)) {
                log.debug("Registering string type for {}", key);
                mapping.field("type", "string");
                if (map==Mapping.STRING)
                    mapping.field("index","not_analyzed");
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
    }

    public XContentBuilder getContent(List<IndexEntry> additions) throws StorageException {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (IndexEntry add : additions) {
                if (add.value instanceof Number) {
                    if (AttributeUtil.isWholeNumber((Number) add.value)) {
                        builder.field(add.field, ((Number) add.value).longValue());
                    } else { //double or float
                        builder.field(add.field, ((Number) add.value).doubleValue());
                    }
                } else if (AttributeUtil.isString(add.value)) {
                    builder.field(add.field, (String) add.value);
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
                } else throw new IllegalArgumentException("Unsupported type: " + add.value);

            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new PermanentStorageException("Could not write json");
        }
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws StorageException {
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
                            bulkrequests++;
                        } else {
                            StringBuilder script = new StringBuilder();
                            for (String key : Iterables.transform(mutation.getDeletions(),IndexMutation.ENTRY2FIELD_FCT)) {
                                script.append("ctx._source.remove(\"" + key + "\"); ");
                                log.trace("Deleting individual field [{}] for document {}", key, docid);
                            }
                            brb.add(client.prepareUpdate(indexName, storename, docid).setScript(script.toString()));
                        }
                    }
                    if (mutation.hasAdditions()) {
                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", docid);
                            brb.add(new IndexRequest(indexName, storename, docid).source(getContent(mutation.getAdditions())));
                            bulkrequests++;
                        } else {
                            boolean needUpsert = !mutation.hasDeletions();
                            XContentBuilder builder = getContent(mutation.getAdditions());
                            UpdateRequestBuilder update = client.prepareUpdate(indexName, storename, docid).setDoc(builder);
                            if (needUpsert) update.setUpsert(builder);
                            log.trace("Updating document {} with upsert {}", docid, needUpsert);
                            brb.add(update);
                        }
                    }

                }
            }
            if (bulkrequests > 0) brb.execute().actionGet();
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
                Mapping map = Mapping.getMapping(informations.get(key));
                if ((map==Mapping.DEFAULT || map==Mapping.TEXT) && !titanPredicate.toString().startsWith("CONTAINS"))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + titanPredicate);
                if (map==Mapping.STRING && titanPredicate.toString().startsWith("CONTAINS"))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + titanPredicate);

                if (titanPredicate == Text.CONTAINS) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.termFilter(key, (String) value);
                } else if (titanPredicate == Text.CONTAINS_PREFIX) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.prefixFilter(key, (String) value);
                } else if (titanPredicate == Text.CONTAINS_REGEX) {
                    value = ((String) value).toLowerCase();
                    return FilterBuilders.regexpFilter(key, (String) value);
                } else if (titanPredicate == Text.PREFIX) {
                    return FilterBuilders.prefixFilter(key, (String) value);
                } else if (titanPredicate == Text.REGEX) {
                    return FilterBuilders.regexpFilter(key, (String) value);
                } else if (titanPredicate == Cmp.EQUAL) {
                    return FilterBuilders.termFilter(key, (String) value);
                } else if (titanPredicate == Cmp.NOT_EQUAL) {
                    return FilterBuilders.notFilter(FilterBuilders.termFilter(key, (String) value));
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
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws StorageException {
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
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws StorageException {
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
            }
        }
        return false;
    }


    @Override
    public boolean supports(KeyInformation information) {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class) {
            if (mapping==Mapping.DEFAULT) return true;
        } else if (AttributeUtil.isString(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.STRING || mapping==Mapping.TEXT) return true;
        }
        return false;
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws StorageException {
        return new DefaultTransaction(config);
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
