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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.internal.core.cql.ResultSets;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.Array;
import io.vavr.collection.Iterator;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.leveledCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.select.Selector.column;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPACT_STORAGE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_BLOCK_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_TYPE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_STRATEGY;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;

/**
 * An implementation of {@link KeyColumnValueStore} which stores the data in a CQL connected backend.
 */
public class CQLKeyColumnValueStore implements KeyColumnValueStore {

    private static final String TTL_FUNCTION_NAME = "ttl";
    private static final String WRITETIME_FUNCTION_NAME = "writetime";

    public static final String KEY_COLUMN_NAME = "key";
    public static final String COLUMN_COLUMN_NAME = "column1";
    public static final String VALUE_COLUMN_NAME = "value";
    static final String WRITETIME_COLUMN_NAME = "writetime";
    static final String TTL_COLUMN_NAME = "ttl";

    private static final String KEY_BINDING = "key";
    private static final String COLUMN_BINDING = "column1";
    private static final String VALUE_BINDING = "value";
    private static final String TIMESTAMP_BINDING = "timestamp";
    private static final String TTL_BINDING = "ttl";
    private static final String SLICE_START_BINDING = "sliceStart";
    private static final String SLICE_END_BINDING = "sliceEnd";
    private static final String KEY_START_BINDING = "keyStart";
    private static final String KEY_END_BINDING = "keyEnd";
    private static final String LIMIT_BINDING = "maxRows";

    static final Function<? super Throwable, BackendException> EXCEPTION_MAPPER = cause -> Match(cause).of(
            Case($(instanceOf(QueryValidationException.class)), PermanentBackendException::new),
            Case($(), TemporaryBackendException::new));

    private final CQLStoreManager storeManager;
    private final ExecutorService executorService;
    private final CqlSession session;
    private final String tableName;
    private final CQLColValGetter getter;
    private final Runnable closer;

    private final PreparedStatement getSlice;
    private final PreparedStatement getKeysAll;
    private final PreparedStatement getKeysRanged;
    private final PreparedStatement deleteColumn;
    private final PreparedStatement insertColumn;
    private final PreparedStatement insertColumnWithTTL;

    /**
     * Creates an instance of the {@link KeyColumnValueStore} that stores the data in a CQL backed table.
     *
     * @param storeManager the {@link CQLStoreManager} that maintains the list of {@link CQLKeyColumnValueStore}s
     * @param tableName the name of the database table for storing the key/column/values
     * @param configuration data used in creating this store
     * @param closer callback used to clean up references to this store in the store manager
     */
    public CQLKeyColumnValueStore(final CQLStoreManager storeManager, final String tableName, final Configuration configuration, final Runnable closer) {

        this.storeManager = storeManager;
        this.executorService = this.storeManager.getExecutorService();
        this.tableName = tableName;
        this.closer = closer;
        this.session = this.storeManager.getSession();
        this.getter = new CQLColValGetter(storeManager.getMetaDataSchema(this.tableName));

        if(shouldInitializeTable()) {
            initializeTable(this.session, this.storeManager.getKeyspaceName(), tableName, configuration, this.storeManager.isCompactStorageAllowed());
        }

        // @formatter:off
        this.getSlice = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .where(
                    Relation.column(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING)),
                    Relation.column(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING)),
                    Relation.column(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
                )
                .limit(bindMarker(LIMIT_BINDING)).build());

        this.getKeysRanged = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .allowFiltering()
                .where(
                    Relation.token(KEY_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(KEY_START_BINDING)),
                    Relation.token(KEY_COLUMN_NAME).isLessThan(bindMarker(KEY_END_BINDING))
                )
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThanOrEqualTo(bindMarker(SLICE_END_BINDING))
                .build());

        this.getKeysAll = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .allowFiltering()
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
                .build());

        this.deleteColumn = this.session.prepare(deleteFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .usingTimestamp(bindMarker(TIMESTAMP_BINDING))
                .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isEqualTo(bindMarker(COLUMN_BINDING))
                .build());

        this.insertColumn = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING))
                .usingTimestamp(bindMarker(TIMESTAMP_BINDING)).build());

        this.insertColumnWithTTL = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING))
                .usingTimestamp(bindMarker(TIMESTAMP_BINDING))
                .usingTtl(bindMarker(TTL_BINDING)).build());
        // @formatter:on
    }

    /**
     * Check if the current table should be initialized.
     * NOTE: This additional check is needed when Cassandra security is enabled, for more info check issue #1103
     * @return true if table already exists in current keyspace, false otherwise
     */
    private boolean shouldInitializeTable() {
        return storeManager.getSession().getMetadata()
            .getKeyspace(storeManager.getKeyspaceName()).map(k -> !k.getTable(this.tableName).isPresent())
            .orElse(true);
    }

    private static void initializeTable(final CqlSession session, final String keyspaceName, final String tableName, final Configuration configuration, final boolean allowCompactStorage) {
        CreateTableWithOptions createTable = createTable(keyspaceName, tableName)
                .ifNotExists()
                .withPartitionKey(KEY_COLUMN_NAME, DataTypes.BLOB)
                .withClusteringColumn(COLUMN_COLUMN_NAME, DataTypes.BLOB)
                .withColumn(VALUE_COLUMN_NAME, DataTypes.BLOB);


        createTable = compactionOptions(createTable, configuration);
        // COMPACT STORAGE is allowed on Cassandra 2 or earlier
        // when COMPACT STORAGE is allowed, the default is to enable it
        boolean useCompactStorage =
            (allowCompactStorage && configuration.has(CF_COMPACT_STORAGE))
            ? configuration.get(CF_COMPACT_STORAGE)
            : allowCompactStorage;

        if(useCompactStorage){
            createTable = createTable.withCompactStorage();
        }

        createTable = compressionOptions(createTable, configuration, allowCompactStorage);


        session.execute(createTable.build());
    }

    private static CreateTableWithOptions compressionOptions(final CreateTableWithOptions createTable, final Configuration configuration, boolean allowCompactStorage) {
        if (!configuration.get(CF_COMPRESSION)) {
            // No compression
            return createTable.withNoCompression();
        }
        String compressionType = configuration.get(CF_COMPRESSION_TYPE);
        int chunkLengthInKb = configuration.get(CF_COMPRESSION_BLOCK_SIZE);
        // AllowCompactStorage is true only when using Cassandra 2.x, which uses 'sstable_compression' instead of 'class' to refer to
        // the compression type Class
        String compressionKey = (allowCompactStorage) ? "sstable_compression" : "class";

        return createTable.withOption("compression",
                ImmutableMap.of(compressionKey, compressionType, "chunk_length_kb", chunkLengthInKb));

    }

    private static CreateTableWithOptions compactionOptions(CreateTableWithOptions createTable, final Configuration configuration) {
        if (!configuration.has(COMPACTION_STRATEGY)) {
            return createTable;
        }

        CompactionStrategy<?> compactionStrategy = Match(configuration.get(COMPACTION_STRATEGY))
                .of(
                        Case($("SizeTieredCompactionStrategy"), leveledCompactionStrategy()),
                        Case($("TimeWindowCompactionStrategy"), timeWindowCompactionStrategy()),
                        Case($("LeveledCompactionStrategy"), leveledCompactionStrategy()));
        Iterator<Array<String>> groupedOptions = Array.of(configuration.get(COMPACTION_OPTIONS))
            .grouped(2);

        for(Array<String> keyValue: groupedOptions){
            compactionStrategy = compactionStrategy.withOption(keyValue.get(0), keyValue.get(1));
        }

        return createTable.withCompaction(compactionStrategy);
    }

    @Override
    public void close() throws BackendException {
        this.closer.run();
    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        final Future<EntryList> result = Future.fromJavaFuture(
                this.executorService,
                this.session.executeAsync(this.getSlice.bind()
                        .setByteBuffer(KEY_BINDING, query.getKey().asByteBuffer())
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setInt(LIMIT_BINDING, query.getLimit())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))
                        .toCompletableFuture())
                .map(resultSet -> fromResultSet(resultSet, this.getter));
        interruptibleWait(result);
        return result.getValue().get().getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("The CQL backend does not support multi-key queries");
    }

    /**
     * VAVR Future.await will throw InterruptedException wrapped in a FatalException. If the Thread was in Object.wait, the interrupted
     * flag will be cleared as a side effect and needs to be reset. This method checks that the underlying cause of the FatalException is
     * InterruptedException and resets the interrupted flag.
     *
     * @param result the future to wait on
     * @throws PermanentBackendException if the thread was interrupted while waiting for the future result
     */
    private void interruptibleWait(final Future<?> result) throws PermanentBackendException {
        try {
            result.await();
        } catch (Exception e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PermanentBackendException(e.getCause());
        }
    }

    private static EntryList fromResultSet(final AsyncResultSet resultSet, final StaticArrayEntry.GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> getter) {
        final Lazy<ArrayList<Row>> lazyList = Lazy.of(() -> Lists.newArrayList(ResultSets.newInstance(resultSet)));

        // Use the Iterable overload of ofByteBuffer as it's able to allocate
        // the byte array up front.
        // To ensure that the Iterator instance is recreated, it is created
        // within the closure otherwise
        // the same iterator would be reused and would be exhausted.
        return StaticArrayEntryList.ofStaticBuffer(() -> Iterator.ofAll(lazyList.get()).map(row -> Tuple.of(
                        StaticArrayBuffer.of(row.getByteBuffer(COLUMN_COLUMN_NAME)),
                        StaticArrayBuffer.of(row.getByteBuffer(VALUE_COLUMN_NAME)),
                        row)),
                getter);
    }

    /*
     * Used from CQLStoreManager
     */
    BatchableStatement<BoundStatement> deleteColumn(final StaticBuffer key, final StaticBuffer column, final long timestamp) {
        return this.deleteColumn.bind()
                .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                .setByteBuffer(COLUMN_BINDING, column.asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

    /*
     * Used from CQLStoreManager
     */
    BatchableStatement<BoundStatement> insertColumn(final StaticBuffer key, final Entry entry, final long timestamp) {
        final Integer ttl = (Integer) entry.getMetaData().get(EntryMetaData.TTL);
        if (ttl != null) {
            return this.insertColumnWithTTL.bind()
                    .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                    .setByteBuffer(COLUMN_BINDING, entry.getColumn().asByteBuffer())
                    .setByteBuffer(VALUE_BINDING, entry.getValue().asByteBuffer())
                    .setLong(TIMESTAMP_BINDING, timestamp)
                    .setInt(TTL_BINDING, ttl);
        }
        return this.insertColumn.bind()
                .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                .setByteBuffer(COLUMN_BINDING, entry.getColumn().asByteBuffer())
                .setByteBuffer(VALUE_BINDING, entry.getValue().asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

    @Override
    public void mutate(final StaticBuffer key, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
        this.storeManager.mutateMany(Collections.singletonMap(this.tableName, Collections.singletonMap(key, new KCVMutation(additions, deletions))), txh);
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) throws BackendException {
        final boolean hasLocking = this.storeManager.getFeatures().hasLocking();
        if (!hasLocking) {
            throw new UnsupportedOperationException(String.format("%s doesn't support locking", getClass()));
        }
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        if (!this.storeManager.getFeatures().hasOrderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when the byteorderedpartitioner is used.");
        }

        TokenMap tokenMap = this.session.getMetadata().getTokenMap().get();
        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.getter,
                this.session.execute(this.getKeysRanged.bind()
                        .setToken(KEY_START_BINDING, tokenMap.newToken(query.getKeyStart().asByteBuffer()))
                        .setToken(KEY_END_BINDING, tokenMap.newToken(query.getKeyEnd().asByteBuffer()))
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setPageSize(this.storeManager.getPageSize())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        if (this.storeManager.getFeatures().hasOrderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when a random partitioner (md5 or murmur3) is used.");
        }

        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.getter,
                this.session.execute(this.getKeysAll.bind()
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setPageSize(this.storeManager.getPageSize())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }
}
