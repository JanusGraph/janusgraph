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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
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
import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_CONNECT_BUCKET;
import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_CONNECT_PASSWORD;
import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_CONNECT_USERNAME;
import static org.janusgraph.diskstorage.couchbase.CouchbaseConfigOptions.CLUSTER_DEFAULT_SCOPE;


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
    private Bucket bucket;
    private final String bucketName;
    private Cluster cluster;
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
        this.bucketName = configuration.get(CLUSTER_CONNECT_BUCKET);
        defaultScopeName = configuration.get(CLUSTER_DEFAULT_SCOPE);
        String parallelism = configuration.get(CLUSTER_PARALLELISM);
        try {
            this.parallelism = Integer.parseInt(parallelism);
            log.info("Using {} for parallel slice fetching", this.parallelism);
        } catch (NumberFormatException nfe) {
            throw new PermanentBackendException("Unable to parse cluster-parallelism setting", nfe);
        }
        if (parallelism == null || parallelism.isEmpty()) {
            throw new PermanentBackendException("Couchbase slice query parallelism is not specified");
        }
    }

    private void connect() throws BackendException {
        if (cluster != null && bucket != null) {
            return;
        }
        Configuration configuration = getStorageConfig();
        String user = configuration.get(CLUSTER_CONNECT_USERNAME);
        String password = configuration.get(CLUSTER_CONNECT_PASSWORD);

        String connectString = configuration.get(CLUSTER_CONNECT_STRING);
        log.info("Connecting to {}", connectString);

        if (connectString == null || connectString.isEmpty()) {
            throw new PermanentBackendException("Couchbase connect string is not specified");
        }
        if (user == null || user.isEmpty()) {
            throw new PermanentBackendException("Couchbase connect user is not specified");
        }
        if (password == null || password.isEmpty()) {
            throw new PermanentBackendException("Couchbase connect password is not specified");
        }

        // open the db or connect to the cluster
        log.trace("Connecting to couchbase cluster '{}' (bucket: `{}`, scope: `{}`)", connectString, bucketName, defaultScopeName);
        cluster = Cluster.connect(connectString, user, password);

        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.parse("PT20S"));
        log.trace("Connected to couchbase cluster");

        String clusterConnectString = configuration.get(CLUSTER_CONNECT_STRING);
        log.info("Couchbase connect string: {}", clusterConnectString);

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
                .transactional(false).locking(false)
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
        if (cluster == null) {
            connect();
        }
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
                        cm.createCollection(defaultScopeName, name);
                        Thread.sleep(500);
                        Collection collection = bucket.scope(defaultScopeName).collection(name);
                        openStoreDbs.put(name, collection);
                        collection.queryIndexes().createPrimaryIndex(
                            CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions()
                                .ignoreIfExists(true)
                                .retryStrategy(
                                    BestEffortRetryStrategy.withExponentialBackoff(
                                        Duration.ofMillis(100),
                                        Duration.ofMillis(30000),
                                        2
                                    )
                                )
                        );
                    }
                }
            }


            CouchbaseKeyColumnValueStore store = new CouchbaseKeyColumnValueStore(this, name, bucketName, defaultScopeName, cluster, parallelism);
            stores.put(name, store);
            log.debug("Opened database collection {}", name);

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
        if (log.isTraceEnabled())
            openManagers.remove(this);

        try {
            if (!stores.isEmpty()) {
                stores.values().forEach(this::closeStore);
            }
        } catch (Exception e) {
            throw new PermanentBackendException("Could not close Couchbase database", e);
        }
        log.info("CouchbaseStoreManager closed");

        if (log.isTraceEnabled()) {
            openManagers.remove(this);
            dumpOpenManagers();
        }
    }


    @Override
    public void clearStorage() throws BackendException {

        connect();
        CollectionManager cm = bucket.collections();
        ScopeSpec defaultScope = cm.getAllScopes().stream().filter(s -> s.name().equals(defaultScopeName)).findFirst().orElse(null);

        if (defaultScope == null) {
            throw new PermanentBackendException("Could not get ScopeSpec for configured scope ");
        }


        for (CollectionSpec cs : defaultScope.collections()) {
            final String collection = cs.name();
            try {
                //According to tests, clear storage is a hard clean process, and should wipe everything
                String query = "DROP COLLECTION `" + bucket.name() + "`.`" + defaultScopeName + "`.`" + collection + "` IF EXISTS";
                log.debug("Running Query: " + query);
                cluster.query(query, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
            } catch (Exception e) {
                throw new PermanentBackendException("Could not clear Couchbase storage", e);
            }
        }

        log.info("CouchbaseStoreManager cleared storage");
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            connect();
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

    public void closeStore(CouchbaseKeyColumnValueStore store) {
        stores.remove(store.getName());
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
                openStoreDbs.get(docMutation.getTable()).upsert(document.id(), document.content(),
                        UpsertOptions.upsertOptions());
            } else {
                if (isNew) {
                    log.warn("Tried to remove Collection={}, Removing id={} but it hasn't been added ", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                } else {
                    log.info("Collection={}, Removing id={}", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                    openStoreDbs.get(docMutation.getTable()).remove(document.id(), RemoveOptions.removeOptions());
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
            List<Tuple2<String, JsonDocument>> upsertList = new ArrayList<>();
            List<Tuple2<String, JsonDocument>> deleteList = new ArrayList<>();

            for (CouchbaseDocumentMutation docMutation : batch) {
                JsonDocument document;
                boolean isNew = false;
                try {
                    GetResult getResult = openStoreDbs.get(docMutation.getTable()).get(docMutation.getDocumentId());
                    document = JsonDocument.create(
                        docMutation.getDocumentId(),
                        getResult.contentAsObject(),
                        getResult.cas()
                    );
                } catch (DocumentNotFoundException ignored) {
                    document = createNewDocument(docMutation);
                    isNew = true;
                }

                Map<String, CouchbaseColumn> columns = getMutatedColumns(docMutation, document);

                if (!columns.isEmpty()) {
                    //argh!
                    updateColumns(document, columns);
                    openStoreDbs.get(docMutation.getTable()).upsert(docMutation.getDocumentId(), document.content());
                    upsertList.add(Tuples.of(docMutation.getTable(), document));
                } else {
                    if (isNew) {
                        log.warn("Tried to remove a document that doesn't exist in the database yet Collection={}, Removing id={}", openStoreDbs.get(docMutation.getTable()).name(), document.id());
                    } else {
                        openStoreDbs.get(docMutation.getTable()).remove(docMutation.getDocumentId());
                        deleteList.add(Tuples.of(docMutation.getTable(), document));
                    }
                }
            }

            log.debug("The following documents have been updated: {}", upsertList.stream().map(e -> e.getT2().id()).collect(Collectors.joining("', '", "['", "'] ")));
            log.debug("The following documents have been deleted: {}", deleteList.stream().map(e -> e.getT2().id()).collect(Collectors.joining("', '", "['", "'] ")));
        }
    }


}
