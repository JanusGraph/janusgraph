/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.couchbase.lucene.Lucene2CouchbaseQLTranslator;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.FixedCondition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_CONNECT_STRING;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_FUZINESS;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;

/**
 * @author : Dmitrii Chechetkin (dmitrii.chechetkin@couchbase.com)
 */
public class CouchbaseIndex implements IndexProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseIndex.class);

    private static final String STRING_MAPPING_SUFFIX = "__STRING";
    static final String FTS_INDEX_NAME = "fulltext_index";
    private final String name;
    private final Cluster cluster;

    private final Bucket bucket;

    private final Scope scope;

    private final int fuzziness;

    private final String indexNamePrefix;

    private final String indexNamespace;

    public CouchbaseIndex(Configuration config) {
        boolean isTLS = false;
        final String connectString = config.get(CLUSTER_CONNECT_STRING);
        if (connectString.startsWith("couchbases://")) {
            isTLS = true;
        }

        ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
                .ioConfig(IoConfig.enableDnsSrv(isTLS))
                .securityConfig(SecurityConfig.enableTls(isTLS)
                        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));

        new ConnectionStringPropertyLoader(connectString).load(envBuilder);

        ClusterEnvironment env = envBuilder.build();
        name = config.get(INDEX_NAME);
        cluster = Cluster.connect(connectString,
                ClusterOptions.clusterOptions(config.get(CLUSTER_CONNECT_USERNAME),
                        config.get(CLUSTER_CONNECT_PASSWORD)).environment(env));

        fuzziness = config.get(CLUSTER_DEFAULT_FUZINESS);

        String bucketName = config.get(CLUSTER_CONNECT_BUCKET);
        String scopeName = config.get(CLUSTER_DEFAULT_SCOPE);

        bucket = cluster.bucket(bucketName);
        scope = bucket.scope(scopeName);
        indexNamePrefix = String.format("%s_%s", bucketName, scopeName);
        indexNamespace = String.format("%s.%s", bucketName, scopeName);
    }

    @Override
    public void register(String storeName, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        ensureStorageExists(storeName);
        CouchbaseIndexTransaction cbitx = (CouchbaseIndexTransaction) tx;
        cbitx.register(storeName, key, information);
    }

    protected Collection getStorage(String name) {
        Collection result = scope.collection(name);
        if (result == null) {
            bucket.collections().createCollection(CollectionSpec.create(name, scope.name()));
            result = scope.collection(name);
        }
        return result;
    }

    protected String getIndexFullName(String name) {
        return indexNamePrefix + "_" + name;
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        mutations.keySet().forEach(this::ensureStorageExists);
        ((CouchbaseIndexTransaction)tx).mutate(mutations, information);
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        documents.keySet().forEach(this::ensureStorageExists);
        ((CouchbaseIndexTransaction)tx).restore(documents, information);
    }

    @Override
    public Number queryAggregation(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx, Aggregation aggregation) throws BackendException {
        final String aggType = aggregation.getType().name().toLowerCase();
        final String fieldName = aggregation.getFieldName() == null ? "*" : aggregation.getFieldName();
        return doQuery(String.format("%s(%s) as __agg_result", aggType, fieldName), query, information, tx)
                .rowsAsObject().stream()
                .findFirst().map(row -> row.getLong("__agg_result"))
                .orElse(0L);
    }

    protected CollectionSpec ensureStorageExists(String name) {
        return getCollection(name).orElseGet(() -> createCollection(name));
    }

    protected Optional<CollectionSpec> getCollection(String name) {
        return bucket.collections().getAllScopes()
                .parallelStream()
                .filter(scopeSpec -> scopeSpec.name().equals(scope.name()))
                .flatMap(scopeSpec -> scopeSpec.collections().parallelStream())
                .filter(collectionSpec -> collectionSpec.name().equals(name))
                .findFirst();
    }

    protected CollectionSpec createCollection(String name) {
        CollectionSpec collectionSpec = CollectionSpec.create(name, scope.name(), Duration.ZERO);
        bucket.collections().createCollection(collectionSpec);

        try {
            Thread.sleep(2000);
            scope.query("CREATE PRIMARY INDEX ON `" + name + "`");
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return collectionSpec;
    }

    protected List<QueryFilter> transformFilter(String storageName, Condition<?> condition) {
        final List<QueryFilter> result = new LinkedList<>();
        if (condition instanceof PredicateCondition) {
            final PredicateCondition<String, ?> atom = (PredicateCondition<String, ?>) condition;
            Object value = atom.getValue();
            final String key = atom.getKey();
            final JanusGraphPredicate predicate = atom.getPredicate();
            final String fullIndexName = getIndexFullName(storageName);
            if (value == null && predicate == Cmp.NOT_EQUAL) {
                result.add(new QueryFilter(String.format("EXISTS %s", key)));
            } else if (predicate == Cmp.EQUAL
                    || predicate == Cmp.NOT_EQUAL
                    || predicate == Cmp.GREATER_THAN
                    || predicate == Cmp.GREATER_THAN_EQUAL
                    || predicate == Cmp.LESS_THAN
                    || predicate == Cmp.LESS_THAN_EQUAL
            ) {
                result.add(new QueryFilter(String.format("%s %s ?", key, predicate), value));
            } else if (predicate == Text.PREFIX || predicate == Text.NOT_PREFIX) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_PREFIX) {
                    statement.append("NOT ");
                }
                statement.append("POSITION(LOWER(")
                        .append(key)
                        .append("), LOWER(?)) = 0");

                result.add(new QueryFilter(statement.toString(), value));
            } else if (predicate == Text.CONTAINS || predicate == Text.NOT_CONTAINS) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_CONTAINS) {
                    statement.append("NOT ");
                }
                statement.append("CONTAINS(LOWER(")
                        .append(key)
                        .append("), LOWER(?))");

                result.add(new QueryFilter(statement.toString(), value));
            } else if ((predicate == Text.REGEX || predicate == Text.NOT_REGEX)) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_REGEX) {
                    statement.append("NOT ");
                }
                statement.append("REGEXP_MATCHES(")
                        .append(key)
                        .append(", ?)");
                result.add(new QueryFilter(statement.toString(), value));
            } else if ((predicate == Text.CONTAINS_REGEX || predicate == Text.NOT_CONTAINS_REGEX)) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_CONTAINS_REGEX) {
                    statement.append("NOT ");
                }
                statement.append("REGEXP_CONTAINS(")
                        .append(key)
                        .append(", ?)");
                result.add(new QueryFilter(statement.toString(), value));
            } else if (predicate instanceof Text) {
                Text textPredicate = (Text) predicate;
                String not = "";
                if (textPredicate.name().toLowerCase(Locale.ROOT).startsWith("not_")) {
                    not = "NOT ";
                }
                result.add(new QueryFilter(
                        not + "SEARCH(?, ?)",
                        fullIndexName,
                        buildSearchQuery(key, predicate, value)
                ));
            } else if (predicate instanceof Geo) {
                result.add(new QueryFilter(
                        "SEARCH(?, ?)",
                        fullIndexName,
                        buildGeoQuery(key, predicate, value)
                ));
            }else {
                throw new IllegalArgumentException("Unsupported predicate: " + predicate.getClass().getCanonicalName());
            }
        } else if (condition instanceof Not) {
            transformFilter(storageName, ((Not<?>) condition).getChild()).stream()
                    .map(qf -> new QueryFilter("NOT (" + qf.query() + ")", qf.arguments()))
                    .forEach(result::add);
        } else if (condition instanceof And || condition instanceof Or) {
            LinkedList<String> statements = new LinkedList<>();
            LinkedList<Object> arguments = new LinkedList<>();

            for (Condition<?> child : condition.getChildren()) {
                StringBuilder childFilter = new StringBuilder();
                transformFilter(storageName, child).forEach(qf -> {
                    childFilter.append(qf.query());
                    arguments.addAll(Arrays.asList(qf.arguments()));
                });
                statements.add(childFilter.toString());
            }
            result.add(new QueryFilter(statements.stream().collect(
                    Collectors.joining(
                            ") " + ((condition instanceof And) ? "AND" : "OR") + " (",
                            " (",
                            ") "
                    )
            ), arguments.toArray()));
        } else if (condition instanceof FixedCondition) {
            result.add(new QueryFilter(condition.toString()));
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition);
        }

        return result;
    }

    private SearchQuery buildGeoQuery(String key, JanusGraphPredicate predicate, Object value) {
        throw new RuntimeException("STUB");
    }

    protected SearchQuery buildSearchQuery(String key, JanusGraphPredicate predicate, Object value) {
        if (predicate == Text.CONTAINS || predicate == Text.NOT_CONTAINS) {
            return SearchQuery.match(String.valueOf(value)).field(key);
        } else if (predicate == Text.CONTAINS_PHRASE || predicate == Text.NOT_CONTAINS_PHRASE) {
            return SearchQuery.matchPhrase(String.valueOf(value)).field(key);
        } else if (predicate == Text.CONTAINS_PREFIX || predicate == Text.NOT_CONTAINS_PREFIX ||
                predicate == Text.PREFIX || predicate == Text.NOT_PREFIX) {
            return SearchQuery.prefix(String.valueOf(value)).field(key);
        } else if (predicate == Text.CONTAINS_REGEX || predicate == Text.NOT_CONTAINS_REGEX) {
            return SearchQuery.regexp(String.valueOf(value)).field(key);
        } else if (predicate == Text.REGEX || predicate == Text.NOT_REGEX) {
            return SearchQuery.regexp(String.valueOf(value)).field(key);
        } else if (predicate == Text.FUZZY ||
                predicate == Text.NOT_FUZZY ||
                predicate == Text.CONTAINS_FUZZY ||
                predicate == Text.NOT_FUZZY) {
            return SearchQuery.match(String.valueOf(value)).field(key).fuzziness(fuzziness);
        }

        throw new IllegalArgumentException("Predicate is not supported: " + predicate);
    }

    protected SearchResult doQuery(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        tx.commit();
        SearchQuery fts = Lucene2CouchbaseQLTranslator.translate(query.getQuery());
        SearchOptions options = SearchOptions.searchOptions()
                .limit(query.getLimit())
                .skip(query.getOffset());

        LOGGER.info("FTS query: %s", fts);
        return cluster.searchQuery(getIndexFullName(query.getStore()), fts, options);
    }

    protected QueryResult doQuery(String select, IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        tx.commit();
        List<QueryFilter> filter = transformFilter(query.getStore(), query.getCondition());
        JsonArray args = JsonArray.create();
        String filterString = filter.stream()
                .peek(qf -> Arrays.stream(qf.arguments()).forEach(args::add))
                .map(qf -> qf.query())
                .collect(Collectors.joining(") AND (", " (", ") "));

        final String n1ql = "SELECT " + select + " FROM " +
                indexNamespace + "." + query.getStore() +
                " WHERE " + filterString +
                ((query.getOrder().size() > 0) ? " ORDER BY " : "") +
                query.getOrder().stream()
                        .filter(order -> StringUtils.isNotBlank(order.getKey()))
                        .map(order -> order.getKey() + " " + order.getOrder().name())
                        .collect(Collectors.joining(", ")) +
                ((query.hasLimit()) ? " LIMIT " + query.getLimit() : "");
        try {
            LOGGER.info("N1QL query: %s", query);
            return cluster.query(n1ql,
                    QueryOptions.queryOptions()
                            .parameters(args)
            );
        } catch (Exception e) {
            LOGGER.error("Query failed: " + n1ql, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return doQuery("__document_key as id", query, information, tx)
                .rowsAsObject().stream()
                .map(row -> row.getString("id"));
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return doQuery(query, information, tx)
                .rows().stream()
                .map(row -> {
                    String docKey = getStorage(query.getStore()).get(row.id()).contentAsObject().getString("__document_key");
                    return new RawQuery.Result<>(docKey, row.score());
                });
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return doQuery(query, information, tx).metaData().metrics().totalRows();
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new CouchbaseIndexTransaction(config, cluster, bucket, scope, indexNamePrefix, indexNamespace);
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clearStorage() throws BackendException {

    }

    @Override
    public void clearStore(String storeName) throws BackendException {

    }

    @Override
    public boolean exists() throws BackendException {
        return true;
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate predicate) {
        final Class<?> type = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (mapping != Mapping.PREFIX_TREE) {
            if (Number.class.isAssignableFrom(type)) {
                return predicate instanceof Cmp;
            } else if (Geoshape.class.isAssignableFrom(type)) {
                return predicate instanceof Geo;
            } else if (AttributeUtils.isString(type)) {
                switch (mapping) {
                    case DEFAULT:
                    case STRING:
                        return predicate instanceof Cmp ||
                                predicate == Text.PREFIX || predicate == Text.NOT_PREFIX ||
                                predicate == Text.REGEX || predicate == Text.NOT_REGEX ||
                                predicate == Text.CONTAINS_REGEX || predicate == Text.NOT_CONTAINS_REGEX ||
                                predicate == Text.CONTAINS || predicate == Text.NOT_CONTAINS;
                    case TEXT:
                        return predicate == Text.CONTAINS || predicate == Text.NOT_CONTAINS ||
                                predicate == Text.CONTAINS_PHRASE || predicate == Text.NOT_CONTAINS_PHRASE ||
                                predicate == Text.CONTAINS_PREFIX || predicate == Text.NOT_CONTAINS_PREFIX ||
                                predicate == Text.CONTAINS_FUZZY || predicate == Text.NOT_CONTAINS_FUZZY ||
                                predicate == Text.PREFIX || predicate == Text.NOT_PREFIX ||
                                predicate == Text.REGEX || predicate == Text.NOT_REGEX ||
                                predicate == Text.FUZZY || predicate == Text.NOT_FUZZY;
                    case TEXTSTRING:
                        return predicate instanceof Cmp || predicate instanceof Text;
                }
            }
        }
        return false;
    }

    @Override
    public boolean supports(KeyInformation information) {
        final Class<?> type = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(type) || type == Date.class || type == Instant.class
                || type == UUID.class || type == Boolean.class) {
            return mapping == Mapping.DEFAULT;
        } else if (Geoshape.class.isAssignableFrom(type)) {
            return mapping == Mapping.DEFAULT;
        } else if (AttributeUtils.isString(type)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.TEXT
                    || mapping == Mapping.TEXTSTRING || mapping == Mapping.STRING;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        IndexProvider.checkKeyValidity(key);
        return key.replaceAll("\\s", "_")
                .replaceAll("\\.", "_")
                .replaceAll("\\?", "_");
    }

    @Override
    public IndexFeatures getFeatures() {
        return new IndexFeatures.Builder()
                .setDefaultStringMapping(Mapping.STRING)
                .supportedStringMappings(Mapping.TEXT, Mapping.TEXTSTRING, Mapping.STRING, Mapping.PREFIX_TREE)
                .setWildcardField("_all")
                .supportsCardinality(Cardinality.SINGLE)
                .supportsCardinality(Cardinality.LIST)
                .supportsCardinality(Cardinality.SET)
                .supportsNanoseconds()
                .supportNotQueryNormalForm()
                .build();
    }
}
