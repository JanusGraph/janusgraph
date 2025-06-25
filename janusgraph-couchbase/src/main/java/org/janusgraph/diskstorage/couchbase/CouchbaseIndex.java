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
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.CreateScopeOptions;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.result.SearchResult;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Namifiable;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.Mutation;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.couchbase.lucene.Lucene2CouchbaseQLTranslator;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author : Dmitrii Chechetkin (dmitrii.chechetkin@couchbase.com)
 */
public class CouchbaseIndex implements IndexProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseIndex.class);

    private static final String STRING_MAPPING_SUFFIX = "__STRING";
    static final String FTS_INDEX_NAME = "fulltext_index";
    private final String name;
    private Cluster cluster;

    private Bucket bucket;

    private Scope scope;

    private final int fuzziness;

    private final String indexNamePrefix;

    private final String indexNamespace;

    private AtomicReference<MutationState> lastMutationState = new AtomicReference<>();

    public CouchbaseIndex(Configuration config) {
        boolean isTLS = false;
        final String connectString = config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING);
        if (connectString.startsWith("couchbases://")) {
            isTLS = true;
        }

        ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
                .ioConfig(IoConfig.enableDnsSrv(isTLS).enableMutationTokens(true))
                .securityConfig(SecurityConfig.enableTls(isTLS)
                        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));

        new ConnectionStringPropertyLoader(connectString).load(envBuilder);

        ClusterEnvironment env = envBuilder.build();
        name = config.get(GraphDatabaseConfiguration.INDEX_NAME);
        String bucketName = config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET);
        String scopeName = config.get(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE);

        LOGGER.info("Connecting Couchbase Index `{}` (bucket: `{}`, scope: `{}`)", connectString, bucketName, scopeName);
        cluster = Cluster.connect(connectString,
                ClusterOptions.clusterOptions(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME),
                        config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD)).environment(env));

        fuzziness = config.get(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_FUZINESS);

        bucket = cluster.bucket(bucketName);
        try {
            bucket.collections().createScope(scopeName);
        } catch (ScopeExistsException see) {
            // ok, we'll reuse it
        }
        scope = bucket.scope(scopeName);
        indexNamePrefix = String.format("%s_%s", bucketName, scopeName);
        indexNamespace = String.format("`%s`.`%s`", bucketName, scopeName);
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
            createCollection(name);
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
                .findFirst().map(row -> {
                    if (aggregation.getType() == Aggregation.Type.SUM) {
                        if (Float.class.isAssignableFrom(aggregation.getDataType()) || Double.class.isAssignableFrom(aggregation.getDataType())) {
                            return row.getDouble("__agg_result");
                        } else {
                            return row.getLong("__agg_result");
                        }
                    } else if (aggregation.getType() == Aggregation.Type.AVG) {
                        return row.getDouble("__agg_result");
                    } else if (aggregation.getDataType() == null) {
                        return row.getLong("__agg_result");
                    } else if (aggregation.getDataType().equals(Long.class)) {
                        return (Number) row.getLong("__agg_result");
                    } else if (aggregation.getDataType().equals(Double.class)) {
                        return (Number) row.getDouble("__agg_result");
                    } else if (aggregation.getDataType().equals(Integer.class)) {
                        return (Number) row.getInt("__agg_result");
                    } else if (aggregation.getDataType().equals(Float.class)) {
                        return row.getDouble("__agg_result").floatValue();
                    }
                    throw new RuntimeException("Unsupported aggregation type `" + aggregation.getType() + "`");
                })
                .orElse(0L);
    }

    protected CollectionSpec ensureStorageExists(String name) {
        return getCollection(name).orElseGet(() -> createCollection(name));
    }

    protected Optional<CollectionSpec> getCollection(String name) {
        return bucket.collections().getAllScopes().stream()
                .filter(scopeSpec -> scopeSpec.name().equals(scope.name()))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .filter(collectionSpec -> collectionSpec.name().equals(name))
                .findFirst();
    }

    protected CollectionSpec createCollection(String name) {
        bucket.collections().createCollection(scope.name(), name);

        try {
            Thread.sleep(500);
            scope.query("CREATE PRIMARY INDEX ON `" + name + "`");
//            Thread.sleep(1000);
            LOGGER.info("Created index collection '{}'", name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getCollection(name).get();
    }

    protected List<QueryFilter> transformFilter(String storageName, Condition<?> condition, KeyInformation.IndexRetriever information) {
        final List<QueryFilter> result = new LinkedList<>();
        if (condition instanceof PredicateCondition) {
            final PredicateCondition<String, ?> atom = (PredicateCondition<String, ?>) condition;
            Object value = atom.getValue();
            final String key = atom.getKey();
            final JanusGraphPredicate predicate = atom.getPredicate();
            final String fullIndexName = getIndexFullName(storageName);
            if (value == null && predicate == Cmp.NOT_EQUAL) {
                result.add(new QueryFilter(String.format("EXISTS `%s`", key)));
            } else if (predicate == Cmp.EQUAL
                    || predicate == Cmp.NOT_EQUAL
                    || predicate == Cmp.GREATER_THAN
                    || predicate == Cmp.GREATER_THAN_EQUAL
                    || predicate == Cmp.LESS_THAN
                    || predicate == Cmp.LESS_THAN_EQUAL
            ) {
                if (predicate == Cmp.EQUAL) {
                    KeyInformation keyInfo = information.get(storageName, key);
                    if (keyInfo.getCardinality().equals(Cardinality.SINGLE)) {
                        result.add(new QueryFilter(String.format("`%s` %s ?", key, predicate), value));
                    } else {
                        result.add(new QueryFilter(String.format("array_contains(`%s`, ?)", key), value));
                    }
                } else {
                    result.add(new QueryFilter(String.format("`%s` %s ?", key, predicate), value));
                }
            } else if (predicate == Text.PREFIX || predicate == Text.NOT_PREFIX) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_PREFIX) {
                    statement.append("NOT ");
                }
                statement.append("POSITION(LOWER(`")
                        .append(key)
                        .append("`), LOWER(?)) = 0");

                result.add(new QueryFilter(statement.toString(), value));
            } else if (predicate == Text.CONTAINS || predicate == Text.NOT_CONTAINS) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_CONTAINS) {
                    statement.append("NOT ");
                }
                statement.append("CONTAINS(LOWER(`")
                        .append(key)
                        .append("`), LOWER(?))");

                result.add(new QueryFilter(statement.toString(), value));
            } else if ((predicate == Text.REGEX || predicate == Text.NOT_REGEX)) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_REGEX) {
                    statement.append("NOT ");
                }
                statement.append("REGEXP_MATCHES(`")
                        .append(key)
                        .append("`, ?)");
                result.add(new QueryFilter(statement.toString(), value));
            } else if ((predicate == Text.CONTAINS_REGEX || predicate == Text.NOT_CONTAINS_REGEX)) {
                StringBuilder statement = new StringBuilder();
                if (predicate == Text.NOT_CONTAINS_REGEX) {
                    statement.append("NOT ");
                }
                statement.append("REGEXP_CONTAINS(`")
                        .append(key)
                        .append("`, ?)");
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
            transformFilter(storageName, ((Not<?>) condition).getChild(), information).stream()
                    .map(qf -> new QueryFilter("NOT (" + qf.query() + ")", qf.arguments()))
                    .forEach(result::add);
        } else if (condition instanceof And || condition instanceof Or) {
            LinkedList<String> statements = new LinkedList<>();
            LinkedList<Object> arguments = new LinkedList<>();

            for (Condition<?> child : condition.getChildren()) {
                StringBuilder childFilter = new StringBuilder();
                transformFilter(storageName, child, information).forEach(qf -> {
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

    protected QueryResult doQuery(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx, boolean countOnly) throws BackendException {
//        tx.commit();
        SearchQuery fts = Lucene2CouchbaseQLTranslator.translate(query.getQuery());
        JsonObject opts = JsonObject.create();
        opts.put("index", getIndexFullName(query.getStore()));
        StringBuilder n1ql = new StringBuilder("SELECT ");
        if (countOnly) {
            n1ql.append("COUNT(id) AS count FROM (");
        } else {
            n1ql.append("data.* FROM (");
        }
        n1ql.append("SELECT META(store).id as id, SEARCH_SCORE(store) as score ");

        QueryOptions qo = QueryOptions.queryOptions();
        if (lastMutationState.get() != null) {
            qo.consistentWith(lastMutationState.get());
        }
//        qo.scanConsistency(QueryScanConsistency.REQUEST_PLUS);

        n1ql.append(String.format(
            "FROM `%s`.`%s`.`%s` as store WHERE SEARCH(store, %s, %s) LIMIT %d OFFSET %d",
            bucket.name(), scope.name(), query.getStore(), fts.export(), opts,  Math.min(9999 - query.getOffset(), query.getLimit()), query.getOffset()
        ));
        n1ql.append(") as data");
        LOGGER.info("fts query: {}", n1ql.toString());
        return cluster.query(n1ql.toString(), qo);
//        return cluster.searchQuery(getIndexFullName(query.getStore()), fts, options);
    }

    protected QueryResult doQuery(String select, IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
//        tx.commit();
        List<QueryFilter> filter = transformFilter(query.getStore(), query.getCondition(), information);
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
            LOGGER.info("N1QL query: {}", n1ql);
            QueryOptions options = QueryOptions.queryOptions()
                    .parameters(args);
            MutationState ms = lastMutationState.get();
            if (ms != null) {
                options.consistentWith(ms);
            }
            return cluster.query(n1ql, options);
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
        return doQuery(query, information, tx, false)
                .rowsAsObject().stream()
                .map(row -> {
//                    String docKey = getStorage(query.getStore()).get(row.id()).contentAsObject().getString("__document_key");
                    return new RawQuery.Result<>(row.getString("id"), row.getDouble("score"));
                });
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return (long) doQuery(query, information, tx, true).rowsAsObject()
            .stream().findFirst().map(row -> row.getLong("count")).orElse(0L);
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new CouchbaseIndexTransaction(config, this, indexNamePrefix);
    }

    @Override
    public void close() throws BackendException {
        LOGGER.info("Couchbase Index closed");
    }

    @Override
    public void clearStorage() throws BackendException {
        LOGGER.info("Clear Storage Requested");
        CollectionManager cm = bucket.collections();
        ScopeSpec defaultScope = cm.getAllScopes().stream().filter(s -> s.name().equals(scope.name())).findFirst().orElse(null);

        if (defaultScope == null) {
            throw new PermanentBackendException("Could not get ScopeSpec for configured scope ");
        }


        for (CollectionSpec cs : defaultScope.collections()) {
            final String collection = cs.name();
            try {
                //According to tests, clear storage is a hard clean process, and should wipe everything
                String query = "DELETE FROM `" + bucket.name() + "`.`" + scope.name() + "`." + collection;
                LOGGER.trace("Running Query: " + query);
                cluster.query(query, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
            } catch (Exception e) {
                throw new RuntimeException("Could not clear Couchbase storage", e);
            }
        }

        LOGGER.info("CouchbaseIndex {} cleared storage", name);
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

    public Cluster getCluster() {
        if (cluster == null) {
            throw new RuntimeException("Cluster is closed");
        }
        return cluster;
    }

    public Bucket getBucket() {
        if (bucket == null) {
            throw new RuntimeException("Cluster is closed");
        }
        return bucket;
    }

    public Scope getScope() {
        if (scope == null) {
            throw new RuntimeException("Cluster is closed");
        }
        return scope;
    }

    public void setMutationState(MutationState mutationState) {
        lastMutationState.set(mutationState);
    }

    public MutationState getMutationState() {
        return lastMutationState.get();
    }
}
