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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.column;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.dateTieredStrategy;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.deflate;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.leveledStrategy;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.lz4;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.noCompression;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.sizedTieredStategy;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.snappy;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_BLOCK_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_TYPE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_STRATEGY;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

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
import org.janusgraph.diskstorage.util.StaticArrayEntry.GetColVal;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions;
import com.datastax.driver.core.schemabuilder.TableOptions.CompressionOptions;
import com.google.common.collect.Lists;

import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.Array;
import io.vavr.collection.Iterator;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;

/**
 * An implementation of {@link KeyColumnValueStore} which stores the data in a CQL connected backend.
 */
public class CQLKeyColumnValueStore implements KeyColumnValueStore {

    private static final String TTL_FUNCTION_NAME = "ttl";
    private static final String WRITETIME_FUNCTION_NAME = "writetime";

    static final String KEY_COLUMN_NAME = "key";
    static final String COLUMN_COLUMN_NAME = "column1";
    static final String VALUE_COLUMN_NAME = "value";
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
            Case($(instanceOf(QueryValidationException.class)), qve -> new PermanentBackendException(qve)),
            Case($(instanceOf(UnsupportedFeatureException.class)), ufe -> new PermanentBackendException(ufe)),
            Case($(), t -> new TemporaryBackendException(t)));

    private final CQLStoreManager storeManager;
    private final ExecutorService executorService;
    private final Session session;
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

        initializeTable(this.session, this.storeManager.getKeyspaceName(), tableName, configuration);

        // @formatter:off
        this.getSlice = this.session.prepare(select()
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .fcall(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .fcall(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .from(this.storeManager.getKeyspaceName(), this.tableName)
                .where(eq(KEY_COLUMN_NAME, bindMarker(KEY_BINDING)))
                .and(gte(COLUMN_COLUMN_NAME, bindMarker(SLICE_START_BINDING)))
                .and(lt(COLUMN_COLUMN_NAME, bindMarker(SLICE_END_BINDING)))
                .limit(bindMarker(LIMIT_BINDING)));

        this.getKeysRanged = this.session.prepare(select()
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .fcall(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .fcall(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .from(this.storeManager.getKeyspaceName(), this.tableName)
                .allowFiltering()
                .where(gte(token(KEY_COLUMN_NAME), bindMarker(KEY_START_BINDING)))
                .and(lt(token(KEY_COLUMN_NAME), bindMarker(KEY_END_BINDING)))
                .and(gte(COLUMN_COLUMN_NAME, bindMarker(SLICE_START_BINDING)))
                .and(lte(COLUMN_COLUMN_NAME, bindMarker(SLICE_END_BINDING))));

        this.getKeysAll = this.session.prepare(select()
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .fcall(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .fcall(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .from(this.storeManager.getKeyspaceName(), this.tableName)
                .allowFiltering()
                .where(gte(COLUMN_COLUMN_NAME, bindMarker(SLICE_START_BINDING)))
                .and(lte(COLUMN_COLUMN_NAME, bindMarker(SLICE_END_BINDING))));

        this.deleteColumn = this.session.prepare(delete()
                .from(this.storeManager.getKeyspaceName(), this.tableName)
                .where(eq(KEY_COLUMN_NAME, bindMarker(KEY_BINDING)))
                .and(eq(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING)))
                .using(timestamp(bindMarker(TIMESTAMP_BINDING))));

        this.insertColumn = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING))
                .using(timestamp(bindMarker(TIMESTAMP_BINDING))));

        this.insertColumnWithTTL = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING))
                .using(timestamp(bindMarker(TIMESTAMP_BINDING)))
                .and(ttl(bindMarker(TTL_BINDING))));
        // @formatter:on
    }

    private static void initializeTable(final Session session, final String keyspaceName, final String tableName, final Configuration configuration) {
        session.execute(createTable(keyspaceName, tableName)
                .ifNotExists()
                .addPartitionKey(KEY_COLUMN_NAME, DataType.blob())
                .addClusteringColumn(COLUMN_COLUMN_NAME, DataType.blob())
                .addColumn(VALUE_COLUMN_NAME, DataType.blob())
                .withOptions()
                .compressionOptions(compressionOptions(configuration))
                .compactionOptions(compactionOptions(configuration))
                .compactStorage());
    }

    private static CompressionOptions compressionOptions(final Configuration configuration) {
        if (!configuration.get(CF_COMPRESSION)) {
            // No compression
            return noCompression();
        }

        return Match(configuration.get(CF_COMPRESSION_TYPE)).of(
                Case($("LZ4Compressor"), lz4()),
                Case($("SnappyCompressor"), snappy()),
                Case($("DeflateCompressor"), deflate()))
                .withChunkLengthInKb(configuration.get(CF_COMPRESSION_BLOCK_SIZE));
    }

    private static CompactionOptions<?> compactionOptions(final Configuration configuration) {
        if (!configuration.has(COMPACTION_STRATEGY)) {
            return null;
        }

        final CompactionOptions<?> compactionOptions = Match(configuration.get(COMPACTION_STRATEGY))
                .of(
                        Case($("SizeTieredCompactionStrategy"), sizedTieredStategy()),
                        Case($("DateTieredCompactionStrategy"), dateTieredStrategy()),
                        Case($("LeveledCompactionStrategy"), leveledStrategy()));
        Array.of(configuration.get(COMPACTION_OPTIONS))
                .grouped(2)
                .forEach(keyValue -> compactionOptions.freeformOption(keyValue.get(0), keyValue.get(1)));
        return compactionOptions;
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
                        .setBytes(KEY_BINDING, query.getKey().asByteBuffer())
                        .setBytes(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setBytes(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setInt(LIMIT_BINDING, query.getLimit())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel())))
                .map(resultSet -> fromResultSet(resultSet, this.getter));
        awaitInterruptibly(result);
        return result.getValue().get().getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("The CQL backend does not support multi-key queries");
    }

    /**
     * Javaslang Future.await will throw InterruptedException wrapped in a FatalException. If the Thread was in Object.wait, the interrupted
     * flag will be cleared as a side effect and needs to be reset. This method checks that the underlying cause of the FatalException is
     * InterruptedException and resets the interrupted flag.
     * 
     * @param result the future to wait on
     * @throws PermanentBackendException if the thread was interrupted while waiting for the future result
     */
    private void awaitInterruptibly(final Future<?> result) throws PermanentBackendException {
        try {
            result.await();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PermanentBackendException(e);
        }
    }

    private static EntryList fromResultSet(final ResultSet resultSet, final GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> getter) {
        final Lazy<ArrayList<Row>> lazyList = Lazy.of(() -> Lists.newArrayList(resultSet));
        // Use the Iterable overload of ofByteBuffer as it's able to allocate
        // the byte array up front.
        // To ensure that the Iterator instance is recreated, it is created
        // within the closure otherwise
        // the same iterator would be reused and would be exhausted.
        return StaticArrayEntryList.ofStaticBuffer(() -> Iterator.ofAll(lazyList.get())
                .<Tuple3<StaticBuffer, StaticBuffer, Row>> map(row -> Tuple.of(
                        StaticArrayBuffer.of(row.getBytes(COLUMN_COLUMN_NAME)),
                        StaticArrayBuffer.of(row.getBytes(VALUE_COLUMN_NAME)),
                        row)),
                getter);
    }

    /*
     * Used from CQLStoreManager
     */
    Statement deleteColumn(final StaticBuffer key, final StaticBuffer column, final long timestamp) {
        return this.deleteColumn.bind()
                .setBytes(KEY_BINDING, key.asByteBuffer())
                .setBytes(COLUMN_BINDING, column.asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

    /*
     * Used from CQLStoreManager
     */
    Statement insertColumn(final StaticBuffer key, final Entry entry, final long timestamp) {
        final Integer ttl = (Integer) entry.getMetaData().get(EntryMetaData.TTL);
        if (ttl != null) {
            return this.insertColumnWithTTL.bind()
                    .setBytes(KEY_BINDING, key.asByteBuffer())
                    .setBytes(COLUMN_BINDING, entry.getColumn().asByteBuffer())
                    .setBytes(VALUE_BINDING, entry.getValue().asByteBuffer())
                    .setLong(TIMESTAMP_BINDING, timestamp)
                    .setInt(TTL_BINDING, ttl);
        }
        return this.insertColumn.bind()
                .setBytes(KEY_BINDING, key.asByteBuffer())
                .setBytes(COLUMN_BINDING, entry.getColumn().asByteBuffer())
                .setBytes(VALUE_BINDING, entry.getValue().asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

    @Override
    public void mutate(final StaticBuffer key, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
        this.storeManager.mutateMany(Collections.singletonMap(this.tableName, Collections.singletonMap(key, new KCVMutation(additions, deletions))), txh);
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        if (!this.storeManager.getFeatures().hasOrderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when the byteorderedpartitioner is used.");
        }

        final Metadata metadata = this.session.getCluster().getMetadata();
        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.getter,
                this.session.execute(this.getKeysRanged.bind()
                        .setToken(KEY_START_BINDING, metadata.newToken(query.getKeyStart().asByteBuffer()))
                        .setToken(KEY_END_BINDING, metadata.newToken(query.getKeyEnd().asByteBuffer()))
                        .setBytes(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setBytes(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setFetchSize(this.storeManager.getPageSize())
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
                        .setBytes(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setBytes(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setFetchSize(this.storeManager.getPageSize())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }
}
