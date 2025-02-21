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
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.google.common.collect.Iterators;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class CouchbaseKeyColumnValueStore implements KeyColumnValueStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseKeyColumnValueStore.class);
    private static final CouchbaseColumnConverter columnConverter = CouchbaseColumnConverter.INSTANCE;
    private final Cluster cluster;
    private final String bucketName;
    private final String scopeName;
    private final String collectionName;
    private final CouchbaseStoreManager storeManager;
    private final CouchbaseGetter entryGetter;
    private final String table;
    private Collection storeDb;

    private int parallelism;
    private ForkJoinPool sliceQueryExecutionPool;

    CouchbaseKeyColumnValueStore(CouchbaseStoreManager storeManager,
                                 String table,
                                 String bucketName,
                                 String scopeName,
                                 Cluster cluster,
                                 int parallelism) {
        this.storeManager = storeManager;
        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.cluster = cluster;
        this.table = table;
        this.collectionName = table;
        this.entryGetter = new CouchbaseGetter(storeManager.getMetaDataSchema(this.table));
        this.parallelism = parallelism;

        if (parallelism > 1) {
            sliceQueryExecutionPool = new ForkJoinPool(parallelism + 1 /* adding one additional thread for the main task */);
        }
    }

    protected void open(Bucket bucket, String scopeName) throws PermanentBackendException {
        try {
            // open the couchbase collection and create if it doesn't exist
            CollectionManager cm = bucket.collections();

            for (ScopeSpec s : cm.getAllScopes()) {
                if (s.name().equals(scopeName)) {
                    boolean found = false;
                    for (CollectionSpec cs : s.collections()) {
                        //log.info("got {} vs existing {} ", name, cs.name());

                        if (cs.name().equals(collectionName)) {
                            LOGGER.debug("Using existing collection " + bucketName + "." + scopeName + "." + cs.name());
                            storeDb = bucket.scope(scopeName).collection(collectionName);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        LOGGER.debug("Creating new collection " + bucket.name() + "." + scopeName + "." + collectionName);
                        CollectionSpec collectionSpec = CollectionSpec.create(collectionName, scopeName, Duration.ZERO);
                        cm.createCollection(collectionSpec);
                        storeDb = bucket.scope(scopeName).collection(collectionName);
                        Thread.sleep(2000);
                        LOGGER.debug("Creating primary index...");
                        //cluster.queryIndexes().createPrimaryIndex("`"+bucketName+"`.`"+defaultScopeName+"`.`"+name+"`", CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));
                        cluster.query("CREATE PRIMARY INDEX ON `default`:`" + bucketName + "`.`" + scopeName + "`.`" + collectionName + "`");
                        Thread.sleep(1000);
                    }
                }
            }

            LOGGER.debug("Opened database collection {}", collectionName);
        } catch (Exception e) {
            throw new PermanentBackendException("Could not open Couchbase data store", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        final List<JsonObject> rows = query(Collections.singletonList(query.getKey()), null, null,
                query.getSliceStart(), query.getSliceEnd()).rowsAsObject();

        if (rows.isEmpty())
            return EntryList.EMPTY_LIST;
        else if (rows.size() == 1) {
            final JsonArray columns = rows.get(0).getArray(CouchbaseColumn.COLUMNS);
            return StaticArrayEntryList.ofBytes(convertAndSortColumns(columns, getLimit(query)), entryGetter);
        } else
            throw new TemporaryBackendException("Multiple rows with the same key.");
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh)
            throws BackendException {
        final Map<StaticBuffer, EntryList> rows = query(keys, null, null,
                query.getSliceStart(), query.getSliceEnd()).rowsAsObject().stream()
                .collect(Collectors.toMap(
                        this::getRowId,
                        row -> StaticArrayEntryList.ofBytes(convertAndSortColumns(row.getArray(CouchbaseColumn.COLUMNS),
                                getLimit(query)), entryGetter)
                ));

        return keys.stream().collect(Collectors.toMap(
                key -> key,
                key -> rows.getOrDefault(key, EntryList.EMPTY_LIST)
        ));
    }

    @Override
    public Map<SliceQuery, Map<StaticBuffer, EntryList>> getMultiSlices(MultiKeysQueryGroups<StaticBuffer, SliceQuery> multiKeysQueryGroups, StoreTransaction txh) throws BackendException {
        if (parallelism > 1) {
            final CompletableFuture<Map<SliceQuery, Map<StaticBuffer, EntryList>>> future = new CompletableFuture<>();
            sliceQueryExecutionPool.execute(() -> {
               future.complete(
                   multiKeysQueryGroups.getQueryGroups().stream()
                       .parallel()
                       .flatMap(queriesForKeysPair -> {
                           List<StaticBuffer> keys = queriesForKeysPair.getKeysGroup();
                           return queriesForKeysPair.getQueries().stream()
                               .parallel()
                               .map(query -> {
                                   try {
                                       return new Object[] {query, getSlice(keys, query, txh)};
                                   } catch (BackendException e) {
                                       throw new RuntimeException(e);
                                   }
                               });
                       })
                       .collect(Collectors.toMap(sliceResult -> (SliceQuery) sliceResult[0], sliceResult -> (Map<StaticBuffer, EntryList>)sliceResult[1]))
               );
            });
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            return KeyColumnValueStore.super.getMultiSlices(multiKeysQueryGroups, txh);
        }
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh)
            throws BackendException {
        final String documentId = columnConverter.toString(key);
        final CouchbaseDocumentMutation docMutation = new CouchbaseDocumentMutation(table, documentId,
                new KCVMutation(additions, deletions));
        storeManager.mutate(docMutation, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return table;
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return executeKeySliceQuery(query.getKeyStart(), query.getKeyEnd(), query.getSliceStart(), query.getSliceEnd(),
                getLimit(query));
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return executeKeySliceQuery(null, null, query.getSliceStart(), query.getSliceEnd(),
                getLimit(query));
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Unsupported multislicesquery ");
    }

    private KeyIterator executeKeySliceQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart,
                                             StaticBuffer sliceEnd, int limit) throws BackendException {
        final QueryResult queryResult = query(null, keyStart, keyEnd, sliceStart, sliceEnd);
        return new RowIterator(queryResult.rowsAsObject().iterator(), limit);
    }

    private QueryResult query(List<StaticBuffer> keys, StaticBuffer keyStart, StaticBuffer keyEnd,
                              StaticBuffer sliceStart, StaticBuffer sliceEnd)
            throws BackendException {
        final long currentTimeMillis = storeManager.currentTimeMillis();
        final StringBuilder select = new StringBuilder("SELECT");
        final StringBuilder where = new StringBuilder(" WHERE table = '" + table + "'");
        final JsonObject placeholderValues = JsonObject.create()
                .put("table", table)
                .put("curtime", currentTimeMillis);

        select.append(" META().id as id,");
        if (keys == null) {
            if (keyStart != null) {
                where.append(" AND META().id >= '").append(keyStart).append("'");
            }

            if (keyEnd != null) {
                where.append(" AND META().id < '").append(keyEnd).append("'");
            }
        }

        select.append(" ARRAY a FOR a IN columns WHEN a.`expire` > ").append(currentTimeMillis);
        where.append(" AND ANY a IN columns SATISFIES a.`expire` > ").append(currentTimeMillis);


        if (sliceStart != null) {
            final String sliceStartString = columnConverter.toString(sliceStart);
            select.append(" AND a.`key` >= '").append(sliceStartString).append("'");
            where.append(" AND a.`key` >= '").append(sliceStartString).append("'");
            //placeholderValues.put("$sliceStart", sliceStartString);
        }

        if (sliceEnd != null) {
            final String sliceEndString = columnConverter.toString(sliceEnd);
            select.append(" AND a.`key` < '").append(sliceEndString).append("'");
            where.append(" AND a.`key` < '").append(sliceEndString).append("'");
            //placeholderValues.put("$sliceEnd", sliceEndString);
        }

        select.append(" END as columns");
        where.append(" END");

        final QueryOptions qOptions = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        //.parameters(placeholderValues);
        select.append(" FROM `").append(bucketName).append("`.`").
                append(scopeName).append("`.`").
                append(collectionName).append("`");

        //The USE KEYS keyword makes the Query Engine use KV, which should improve performance significantly
        //However, this clause must be placed between the from and where.
        if(keys != null) {

            if (keys.size() == 1) {
                select.append(" USE KEYS '").append(columnConverter.toString(keys.get(0))).append("' ");
            } else {
                select.append(" USE KEYS ");
                select.append(keys.stream()
                        .map(CouchbaseColumnConverter::toString)
                        .collect(Collectors.joining("', '", "['", "'] ")));
            }
        }

        select.append(where);

        try {
            LOGGER.debug("Couchbase Query: {}", select.toString());
            //logger.info("   and parameters: {}", placeholderValues.toString());

            return cluster.query(select.toString(), qOptions);
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }
    }

    private StaticBuffer getRowId(JsonObject row) {
        return columnConverter.toStaticBuffer(row.getString(CouchbaseColumn.ID));
    }

    private int getLimit(SliceQuery query) {
        return query.hasLimit() ? query.getLimit() : 0;
    }

    private List<CouchbaseColumn> convertAndSortColumns(JsonArray columnsArray, int limit) {
        final Iterator itr = columnsArray.iterator();
        final List<CouchbaseColumn> columns = new ArrayList<>(columnsArray.size());

        int i = 1;
        while (itr.hasNext()) {
            final JsonObject column = (JsonObject) itr.next();
            columns.add(new CouchbaseColumn(
                    column.getString(CouchbaseColumn.KEY),
                    column.getString(CouchbaseColumn.VALUE),
                    column.getLong(CouchbaseColumn.EXPIRE),
                    column.getInt(CouchbaseColumn.TTL)));
            LOGGER.debug(i + "." + column);
            i++;

        }

        columns.sort(Comparator.naturalOrder());


        return limit == 0 || limit >= columns.size() ? columns : columns.subList(0, limit);
    }

    public Collection getCollection() {
        return storeDb;
    }

    public CouchbaseKeyColumnValueStore ensureOpen() throws PermanentBackendException {
        if (storeDb == null) {
            open(cluster.bucket(bucketName), scopeName);
        }
        return this;
    }

    private static class CouchbaseGetter implements StaticArrayEntry.GetColVal<CouchbaseColumn, byte[]> {

        private static final CouchbaseColumnConverter columnConverter = CouchbaseColumnConverter.INSTANCE;
        private final EntryMetaData[] schema;

        private CouchbaseGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public byte[] getColumn(CouchbaseColumn column) {
            return columnConverter.toByteArray(column.getKey());
        }

        @Override
        public byte[] getValue(CouchbaseColumn column) {
            return columnConverter.toByteArray(column.getValue());
        }

        @Override
        public EntryMetaData[] getMetaSchema(CouchbaseColumn column) {
            return schema;
        }

        @Override
        public Object getMetaData(CouchbaseColumn column, EntryMetaData meta) {
            switch (meta) {
                case TIMESTAMP:
                    return column.getExpire() - column.getTtl() * 1000L;
                case TTL:
                    final int ttl = column.getTtl();
                    return ttl == Integer.MAX_VALUE ? 0 : ttl;
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

    private class RowIterator implements KeyIterator {
        private final Iterator<JsonObject> rows;
        private JsonObject currentRow;
        private boolean isClosed;
        private final int limit;

        public RowIterator(Iterator<JsonObject> rowIterator, int limit) {
            this.limit = limit;
            this.rows = Iterators.filter(rowIterator,
                    row -> null != row && null != row.getString(CouchbaseColumn.ID));
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Entry> columns =
                        StaticArrayEntryList.ofBytes(
                                convertAndSortColumns(currentRow.getArray(CouchbaseColumn.COLUMNS), limit),
                                entryGetter).reuseIterator();

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return columns.next();
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            currentRow = rows.next();
            return getRowId(currentRow);
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }
}
