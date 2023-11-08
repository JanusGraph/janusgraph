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
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_CONNECT_STRING;
import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_PARALLELISM;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME;
import static org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE;


/**
 * Couchbase storage manager implementation.
 *
 * @author Jagadesh Munta (jagadesh.munta@couchbase.com)
 */
public class CouchbaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseStoreManager.class);

    private final Map<String, CouchbaseKeyColumnValueStore> stores;
    private static final ConcurrentHashMap<CouchbaseStoreManager, Throwable> openManagers = new ConcurrentHashMap<>();
    //protected final StoreFeatures features;
    protected Configuration config;
    private final Bucket bucket;
    private final String bucketName;
    private final Cluster cluster;
    private final String defaultScopeName;

    private final int parallelism;
    private static final ConcurrentHashMap<String, Collection> openStoreDbs = new ConcurrentHashMap<String, Collection>();
    public static final int PORT_DEFAULT = 8091;  // Not used. Just for the parent constructor.
    private static final CouchbaseColumnConverter columnConverter = CouchbaseColumnConverter.INSTANCE;
    public static final TimestampProviders PREFERRED_TIMESTAMPS = TimestampProviders.MILLI;


    public CouchbaseStoreManager(Configuration configuration) throws BackendException {
        super(configuration, PORT_DEFAULT);
        this.config = configuration;
        stores = new ConcurrentHashMap<>();
        String connectString = configuration.get(CLUSTER_CONNECT_STRING);
        String user = configuration.get(CLUSTER_CONNECT_USERNAME);
        String password = configuration.get(CLUSTER_CONNECT_PASSWORD);
        String parallelism = configuration.get(CLUSTER_PARALLELISM);
        this.bucketName = configuration.get(CLUSTER_CONNECT_BUCKET);

        defaultScopeName = configuration.get(CLUSTER_DEFAULT_SCOPE);

        if (connectString == null || connectString.isEmpty()) {
            throw new PermanentBackendException("Couchbase connect string is not specified");
        }
        if (user == null || user.isEmpty()) {
            throw new PermanentBackendException("Couchbase connect user is not specified");
        }
        if (password == null || password.isEmpty()) {
            throw new PermanentBackendException("Couchbase connect password is not specified");
        }
        if (parallelism == null || parallelism.isEmpty()) {
            throw new PermanentBackendException("Couchbase slice query parallelism is not specified");
        }

        // open the db or connect to the cluster
        boolean isTLS = false;
        if (configuration.get(CLUSTER_CONNECT_STRING).startsWith("couchbases://")) {
            isTLS = true;
        }
        ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
                .ioConfig(IoConfig.enableDnsSrv(isTLS))
                .ioConfig(IoConfig.networkResolution(NetworkResolution.DEFAULT))
                .securityConfig(SecurityConfig.enableTls(isTLS)
                        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
        new ConnectionStringPropertyLoader(connectString).load(envBuilder);

        ClusterEnvironment env = envBuilder.build();
        log.trace("Connecting to couchbase cluster");


        cluster = Cluster.connect(connectString,
                ClusterOptions.clusterOptions(user, password).environment(env));

        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.parse("PT10S"));
        log.trace("Connected to couchbase cluster");

        String clusterConnectString = configuration.get(CLUSTER_CONNECT_STRING);
        log.info("Couchbase connect string: {}", clusterConnectString);

        try {
            this.parallelism = Integer.parseInt(parallelism);
            log.info("Using {} for parallel slice fetching", this.parallelism);
        } catch (NumberFormatException nfe) {
            throw new PermanentBackendException("Unable to parse cluster-parallelism setting", nfe);
        }

        /*features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .supportsInterruption(false)
                    .optimisticLocking(true)
                    .multiQuery(true)
                    .build();
        */
        if (log.isTraceEnabled()) {
            openManagers.put(this, new Throwable("Manager Opened"));
            dumpOpenManagers();
        }
        log.info("CouchbaseStoreManager initialized");
    }

    @Override
    public StoreFeatures getFeatures() {

        Configuration c = GraphDatabaseConfiguration.buildGraphConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true).unorderedScan(true).batchMutation(true)
                .multiQuery(true).distributed(true).keyOrdered(true)
                .cellTTL(true).timestamps(true).preferredTimestamps(PREFERRED_TIMESTAMPS)
                .optimisticLocking(true).keyConsistent(c);

        try {
            fb.localKeyPartition(getDeployment() == Deployment.LOCAL);
        } catch (Exception e) {
            log.warn("Unexpected exception during getDeployment()", e);
        }

        return fb.build();
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

    @Override
    public String toString() {
        return "couchbase[" + bucketName + "@" + super.toString() + "]";
    }


    /*@Override
    public StoreFeatures getFeatures() {
        return features;
    }*/

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    public void dumpOpenManagers() {
        int estimatedSize = stores.size();
        log.trace("---- Begin open Couchbase store manager list ({} managers) ----", estimatedSize);
        for (CouchbaseStoreManager m : openManagers.keySet()) {
            log.trace("Manager {} opened at:", m, openManagers.get(m));
        }
        log.trace("----   End open Couchbase store manager list ({} managers)  ----", estimatedSize);
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        try {

            final StoreTransaction cbTx = new CouchbaseTx(cluster, txCfg);

            if (log.isTraceEnabled()) {
                log.trace("Couchbase tx created", new TransactionBegin(cbTx.toString()));
            }

            return cbTx;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not start Couchbase transactions", e);
        }
    }

    @Override
    public CouchbaseKeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            // open the couchbase collection and create if it doesn't exist
            CollectionManager cm = bucket.collections();

            for (ScopeSpec s : cm.getAllScopes()) {
                if (s.name().equals(defaultScopeName)) {
                    boolean found = false;
                    for (CollectionSpec cs : s.collections()) {
                        //log.info("got {} vs existing {} ", name, cs.name());

                        if (cs.name().equals(name)) {
                            log.info("Using existing collection " + bucket.name() + "." + defaultScopeName + "." + cs.name());
                            openStoreDbs.put(name, bucket.scope(defaultScopeName).collection(name));
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        log.info("Creating new collection " + bucket.name() + "." + defaultScopeName + "." + name);
                        CollectionSpec collectionSpec = CollectionSpec.create(name, defaultScopeName, Duration.ZERO);
                        cm.createCollection(collectionSpec);
                        openStoreDbs.put(name, bucket.scope(defaultScopeName).collection(name));
                        Thread.sleep(2000);
                        log.info("Creating primary index...");
                        //cluster.queryIndexes().createPrimaryIndex("`"+bucketName+"`.`"+defaultScopeName+"`.`"+name+"`", CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));
                        cluster.query("CREATE PRIMARY INDEX ON `default`:`" + bucketName + "`.`" + defaultScopeName + "`.`" + name + "`");
                        Thread.sleep(1000);
                    }
                }
            }

            log.debug("Opened database collection {}", name);

            CouchbaseKeyColumnValueStore store = new CouchbaseKeyColumnValueStore(this, name, bucketName, defaultScopeName, cluster, parallelism);
            stores.put(name, store);
            return store;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not open Couchbase data store", e);
        }
    }

    void removeDatabase(CouchbaseKeyColumnValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        // Remove the couchbase collection
        CollectionManager cm = bucket.collections();
        for (ScopeSpec s : cm.getAllScopes()) {
            if (s.name().equals(defaultScopeName)) {
                for (CollectionSpec cs : s.collections()) {
                    if (cs.name().equals(name)) {
                        log.trace("Dropping collection " + bucket.name() + "." + defaultScopeName + "." + cs.name());
                        cm.dropCollection(cs);
                        break;
                    }
                }
            }
        }
        if (log.isTraceEnabled()) {
            openManagers.remove(this);
        }
        log.debug("Removed database {}", name);
    }

    @Override
    public void close() throws BackendException {
        stores.clear();
        if (log.isTraceEnabled())
            openManagers.remove(this);

        try {
            // TBD: Whether to close or not the cluster itself is a bit of a question.
            cluster.disconnect();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not close Couchbase database", e);
        }
        log.info("CouchbaseStoreManager closed");
    }


    @Override
    public void clearStorage() throws BackendException {

        for (String collection : openStoreDbs.keySet()) {
            try {
                //According to tests, clear storage is a hard clean process, and should wipe everything
                String query = "DROP COLLECTION `" + bucket.name() + "`.`" + defaultScopeName + "`." + collection;
                log.trace("Running Query: " + query);
                QueryResult result = cluster.query(query, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));

            } catch (Exception e) {
                throw new PermanentBackendException("Could not clear Couchbase storage", e);
            }
        }

        log.info("CouchbaseStoreManager cleared storage");
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            CollectionManager cm = bucket.collections();
            for (ScopeSpec s : cm.getAllScopes()) {
                if (s.name().equals(defaultScopeName)) {
                    //if there are more than two collections (_default and ulog_test) it means that the storege exists
                    if (s.collections().size() > 2) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }


    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }

    /* Helper */
    public void mutate(CouchbaseDocumentMutation docMutation, StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        try {
            JsonDocument document = getMutatedDocument(docMutation);
            boolean isNew = false;
            if (document == null) {
                isNew = true;
                document = createNewDocument(docMutation);
            }

            final Map<String, CouchbaseColumn> columns = getMutatedColumns(docMutation, document);

            if (!columns.isEmpty()) {
                updateColumns(document, columns);
                log.info("Collection={}, Mutating id={}", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                if (log.isDebugEnabled()) {
                    log.debug("content={}", document.content());
                }
                openStoreDbs.get(docMutation.getTable()).upsert(document.id(), document.content());
                //storeDb.upsert(document.id(), document.content());
            } else {
                if (isNew) {
                    log.warn("Tried to remove Collection={}, Removing id={} but it hasn't been added ", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                } else {
                    log.info("Collection={}, Removing id={}", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                    openStoreDbs.get(docMutation.getTable()).remove(document.id());
                }
                //storeDb.remove(document.id());
            }
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }

        //sleepAfterWrite(commitTime);
    }

    private JsonDocument getMutatedDocument(CouchbaseDocumentMutation docMutation) {
        // we should get whole document to clean up expired columns otherwise we could mutate document's fragments
        JsonObject documentObj = null;
        try {
            documentObj = openStoreDbs.get(docMutation.getTable()).get(docMutation.getDocumentId()).contentAsObject();
        } catch (CouchbaseException e) {
            log.warn("Document {} not found table=" + docMutation.getTable() + "", docMutation.getHashId());
            return null;
        }

        return JsonDocument.create(docMutation.getDocumentId(), documentObj);
    }

    private JsonDocument createNewDocument(CouchbaseDocumentMutation docMutation) {
        return JsonDocument.create(
                docMutation.getHashId(),
                JsonObject.create()
                        .put(CouchbaseColumn.ID, docMutation.getDocumentId())
                        .put(CouchbaseColumn.TABLE, docMutation.getTable())
                        .put(CouchbaseColumn.COLUMNS, JsonArray.create()));

    }

    private Map<String, CouchbaseColumn> getMutatedColumns(CouchbaseDocumentMutation docMutation,
                                                           JsonDocument document) {
        final long currentTimeMillis = currentTimeMillis();

        final Map<String, CouchbaseColumn> columns = getColumnsFromDocument(document, currentTimeMillis);
        final KCVMutation mutation = docMutation.getMutation();

        if (mutation.hasAdditions()) {
            for (Entry e : mutation.getAdditions()) {
                final int ttl = getTtl(e);
                final String key = columnConverter.toString(e.getColumn());
                columns.put(key, new CouchbaseColumn(key,
                        columnConverter.toString(e.getValue()), getExpire(currentTimeMillis, ttl), ttl));
            }
        }

        if (mutation.hasDeletions()) {
            for (StaticBuffer b : mutation.getDeletions())
                columns.remove(columnConverter.toString(b));
        }

        return columns;
    }

    private long getExpire(long writetime, int ttl) {
        return writetime + ttl * 1000L;
    }

    private int getTtl(Entry e) {
        final Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);
        return null != ttl && ttl > 0 ? ttl : Integer.MAX_VALUE;
    }

    private void updateColumns(JsonDocument document, Map<String, CouchbaseColumn> columns) {
        final List<JsonObject> columnsList = columns.entrySet().stream().map(entry ->
                JsonObject.create()
                        .put(CouchbaseColumn.KEY, entry.getKey())
                        .put(CouchbaseColumn.VALUE, entry.getValue().getValue())
                        .put(CouchbaseColumn.EXPIRE, entry.getValue().getExpire())
                        .put(CouchbaseColumn.TTL, entry.getValue().getTtl())
        ).collect(Collectors.toList());

        document.content().put(CouchbaseColumn.COLUMNS, JsonArray.from(columnsList));
    }

    private Map<String, CouchbaseColumn> getColumnsFromDocument(JsonDocument document, long currentTimeMillis) {
        final Map<String, CouchbaseColumn> columns = new HashMap<>();
        final Iterator it = document.content().getArray(CouchbaseColumn.COLUMNS).iterator();

        while (it.hasNext()) {
            final JsonObject column = (JsonObject) it.next();
            final long expire = column.getLong(CouchbaseColumn.EXPIRE);

            if (expire > currentTimeMillis) {
                final String key = column.getString(CouchbaseColumn.KEY);
                columns.put(key, new CouchbaseColumn(key, column.getString(CouchbaseColumn.VALUE), expire,
                        column.getInt(CouchbaseColumn.TTL)));
            }
        }

        return columns;
    }

    private List<CouchbaseDocumentMutation> convertToDocumentMutations(Map<String, Map<StaticBuffer, KCVMutation>> batch) {
        final List<CouchbaseDocumentMutation> documentMutations = new ArrayList<>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchEntry : batch.entrySet()) {
            final String table = batchEntry.getKey();
            Preconditions.checkArgument(stores.containsKey(table), "Table cannot be found: " + table);

            final Map<StaticBuffer, KCVMutation> mutations = batchEntry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                final KCVMutation mutation = ent.getValue();
                final String id = columnConverter.toString(ent.getKey());
                documentMutations.add(new CouchbaseDocumentMutation(table, id, mutation));
            }
        }

        return documentMutations;
    }

    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }


    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> jbatch, StoreTransaction txh)
            throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        final List<CouchbaseDocumentMutation> documentMutations = convertToDocumentMutations(jbatch);

        Iterable<List<CouchbaseDocumentMutation>> batches = Iterables.partition(documentMutations, 100);

        for (List<CouchbaseDocumentMutation> batch : batches) {
            List<String> newObj = new ArrayList<>();

            List<JsonDocument> temp = Flux.fromIterable(documentMutations)
                    .flatMap(document -> openStoreDbs.get(document.getTable()).reactive().get(document.getDocumentId())
                            .flatMap(doc -> Mono.just(JsonDocument.create(document.getDocumentId(), doc.contentAsObject())))


                    )
                    .onErrorContinue((err, i) -> {
                        log.info("==========Mutation tried to load a document that doesn't exist {}", i);
                    })
                    .collectList()
                    .block();

            Map<String, JsonDocument> results = temp.stream().collect(Collectors.toMap(JsonDocument::id, e -> e));

            List<Tuple2<String, JsonDocument>> upsertList = new ArrayList<>();
            List<Tuple2<String, JsonDocument>> deleteList = new ArrayList<>();

            for (CouchbaseDocumentMutation docMutation : batch) {
                if (results.get(docMutation.getDocumentId()) == null) {
                    newObj.add(docMutation.getDocumentId());
                    results.put(docMutation.getDocumentId(), createNewDocument(docMutation));
                }

                JsonDocument document = results.get(docMutation.getDocumentId());
                Map<String, CouchbaseColumn> columns = getMutatedColumns(docMutation, document);

                if (!columns.isEmpty()) {
                    //argh!
                    updateColumns(document, columns);
                    upsertList.add(Tuples.of(docMutation.getTable(), document));
                } else {
                    if (newObj.contains(document.id())) {
                        log.warn("Tried to remove a document that doesn't exist in the database yet Collection={}, Removing id={}", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                    } else {
                        deleteList.add(Tuples.of(docMutation.getTable(), document));
                    }
                }
            }

            //bulk updates
            Flux.fromIterable(upsertList)
                    .flatMap(tuple -> openStoreDbs.get(tuple.getT1()).reactive().upsert(
                                    tuple.getT2().id(), tuple.getT2().content()
                            )
                    )
                    .collectList()
                    .block();

            log.debug("The following documents have been update: {}", upsertList.stream().map(e -> e.getT2().id()).collect(Collectors.joining("', '", "['", "'] ")));

            //bulk deletes
            Flux.fromIterable(deleteList)
                    .flatMap(tuple -> openStoreDbs.get(tuple.getT1()).reactive().remove(tuple.getT2().id())
                    )
                    .collectList()
                    .block();
            log.debug("The following documents have been deleted: {}", deleteList.stream().map(e -> e.getT2().id()).collect(Collectors.joining("', '", "['", "'] ")));

        }
    }


}
