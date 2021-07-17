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
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.Array;
import io.vavr.collection.Iterator;
import io.vavr.control.Try;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.function.slice.CQLExecutorServiceSliceFunction;
import org.janusgraph.diskstorage.cql.function.slice.CQLSimpleSliceFunction;
import org.janusgraph.diskstorage.cql.function.slice.CQLSliceFunction;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.leveledCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.sizeTieredCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.select.Selector.column;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_BLOCK_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_TYPE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_STRATEGY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.GC_GRACE_SECONDS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SPECULATIVE_RETRY;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORE_META_TIMESTAMPS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORE_META_TTL;

/**
 * An implementation of {@link KeyColumnValueStore} which stores the data in a CQL connected backend.
 */
public class CQLKeyColumnValueStore implements KeyColumnValueStore {

    public static final String TTL_FUNCTION_NAME = "ttl";
    public static final String WRITETIME_FUNCTION_NAME = "writetime";

    public static final String KEY_COLUMN_NAME = "key";
    public static final String COLUMN_COLUMN_NAME = "column1";
    public static final String VALUE_COLUMN_NAME = "value";
    public static final String WRITETIME_COLUMN_NAME = "writetime";
    public static final String TTL_COLUMN_NAME = "ttl";

    public static final String KEY_BINDING = "key";
    public static final String COLUMN_BINDING = "column1";
    public static final String VALUE_BINDING = "value";
    public static final String TIMESTAMP_BINDING = "timestamp";
    public static final String TTL_BINDING = "ttl";
    public static final String SLICE_START_BINDING = "sliceStart";
    public static final String SLICE_END_BINDING = "sliceEnd";
    public static final String KEY_START_BINDING = "keyStart";
    public static final String KEY_END_BINDING = "keyEnd";
    public static final String LIMIT_BINDING = "maxRows";

    public static final Function<? super Throwable, BackendException> EXCEPTION_MAPPER = cause -> Match(cause).of(
            Case($(instanceOf(QueryValidationException.class)), PermanentBackendException::new),
            Case($(), TemporaryBackendException::new));

    private final CQLStoreManager storeManager;
    private final CqlSession session;
    private final String tableName;
    private final CQLColValGetter getter;
    private final Runnable closer;

    private final PreparedStatement getKeysAll;
    private final PreparedStatement getKeysRanged;
    private final PreparedStatement deleteColumn;
    private final PreparedStatement insertColumn;
    private final PreparedStatement insertColumnWithTTL;

    private final CQLSliceFunction cqlSliceFunction;

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

        this.tableName = tableName;
        this.closer = closer;
        this.session = this.storeManager.getSession();
        this.getter = new CQLColValGetter(storeManager.getMetaDataSchema(this.tableName));

        if(shouldInitializeTable()) {
            initializeTable(this.session, this.storeManager.getKeyspaceName(), tableName, configuration);
        }

        // @formatter:off
        final Select getSliceSelect = selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
            .column(COLUMN_COLUMN_NAME)
            .column(VALUE_COLUMN_NAME)
            .where(
                Relation.column(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING)),
                Relation.column(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING)),
                Relation.column(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
            )
            .limit(bindMarker(LIMIT_BINDING));
        PreparedStatement getSlice = this.session.prepare(addTTLFunction(addTimestampFunction(getSliceSelect)).build());

        if (this.storeManager.getFeatures().hasOrderedScan()) {
            final Select getKeysRangedSelect = selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .allowFiltering()
                .where(
                    Relation.token(KEY_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(KEY_START_BINDING)),
                    Relation.token(KEY_COLUMN_NAME).isLessThan(bindMarker(KEY_END_BINDING))
                )
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThanOrEqualTo(bindMarker(SLICE_END_BINDING));
            this.getKeysRanged = this.session.prepare(addTTLFunction(addTimestampFunction(getKeysRangedSelect)).build());
        } else {
            this.getKeysRanged = null;
        }

        if (this.storeManager.getFeatures().hasUnorderedScan()) {
            final Select getKeysAllSelect = selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .allowFiltering()
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThanOrEqualTo(bindMarker(SLICE_END_BINDING));
            this.getKeysAll = this.session.prepare(addTTLFunction(addTimestampFunction(getKeysAllSelect)).build());
        } else {
            this.getKeysAll = null;
        }

        final DeleteSelection deleteSelection = addUsingTimestamp(deleteFrom(this.storeManager.getKeyspaceName(), this.tableName));
        this.deleteColumn = this.session.prepare(deleteSelection
                .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isEqualTo(bindMarker(COLUMN_BINDING))
                .build());

        final Insert insertColumnInsert = addUsingTimestamp(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING)));
        this.insertColumn = this.session.prepare(insertColumnInsert.build());

        if (storeManager.getFeatures().hasCellTTL()) {
            this.insertColumnWithTTL = this.session.prepare(insertColumnInsert.usingTtl(bindMarker(TTL_BINDING)).build());
        } else {
            this.insertColumnWithTTL = null;
        }

        Optional<ExecutorService> executorService = this.storeManager.getExecutorService();

        if(executorService.isPresent()){
            cqlSliceFunction = new CQLExecutorServiceSliceFunction(session, getSlice, getter, executorService.get());
        } else {
            cqlSliceFunction = new CQLSimpleSliceFunction(session, getSlice, getter);
        }

        // @formatter:on
    }

    private DeleteSelection addUsingTimestamp(DeleteSelection deleteSelection) {
        if (storeManager.isAssignTimestamp()) {
            return deleteSelection.usingTimestamp(bindMarker(TIMESTAMP_BINDING));
        }
        return deleteSelection;
    }

    private Insert addUsingTimestamp(Insert insert) {
        if (storeManager.isAssignTimestamp()) {
            return insert.usingTimestamp(bindMarker(TIMESTAMP_BINDING));
        }
        return insert;
    }

    /**
     * Add WRITETIME function into the select query to retrieve the timestamp that the data was written to the database,
     * if {@link STORE_META_TIMESTAMPS} is enabled.
     * @param select original query
     * @return new query
     */
    private Select addTimestampFunction(Select select) {
        if (storeManager.getStorageConfig().get(STORE_META_TIMESTAMPS, this.tableName)) {
            return select.function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME);
        }
        return select;
    }

    /**
     * Add TTL function into the select query to retrieve how much longer the data is going to live, if {@link STORE_META_TTL}
     * is enabled.
     * @param select original query
     * @return new query
     */
    private Select addTTLFunction(Select select) {
        if (storeManager.getStorageConfig().get(STORE_META_TTL, this.tableName)) {
            return select.function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME);
        }
        return select;
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

    private static void initializeTable(final CqlSession session, final String keyspaceName, final String tableName, final Configuration configuration) {
        CreateTableWithOptions createTable = createTable(keyspaceName, tableName)
                .ifNotExists()
                .withPartitionKey(KEY_COLUMN_NAME, DataTypes.BLOB)
                .withClusteringColumn(COLUMN_COLUMN_NAME, DataTypes.BLOB)
                .withColumn(VALUE_COLUMN_NAME, DataTypes.BLOB);

        createTable = compactionOptions(createTable, configuration);
        createTable = compressionOptions(createTable, configuration);
        createTable = gcGraceSeconds(createTable, configuration);
        createTable = speculativeRetryOptions(createTable, configuration);

        session.execute(createTable.build());
    }

    private static CreateTableWithOptions compressionOptions(final CreateTableWithOptions createTable,
                                                             final Configuration configuration) {
        if (!configuration.get(CF_COMPRESSION)) {
            // No compression
            return createTable.withNoCompression();
        }

        String compressionType = configuration.get(CF_COMPRESSION_TYPE);
        int chunkLengthInKb = configuration.get(CF_COMPRESSION_BLOCK_SIZE);

        return createTable.withOption("compression",
            ImmutableMap.of("sstable_compression", compressionType, "chunk_length_kb", chunkLengthInKb));
    }

    private static CreateTableWithOptions compactionOptions(final CreateTableWithOptions createTable,
                                                            final Configuration configuration) {
        if (!configuration.has(COMPACTION_STRATEGY)) {
            return createTable;
        }

        CompactionStrategy<?> compactionStrategy = Match(configuration.get(COMPACTION_STRATEGY))
            .of(
                Case($("SizeTieredCompactionStrategy"), sizeTieredCompactionStrategy()),
                Case($("TimeWindowCompactionStrategy"), timeWindowCompactionStrategy()),
                Case($("LeveledCompactionStrategy"), leveledCompactionStrategy()));
        Iterator<Array<String>> groupedOptions = Array.of(configuration.get(COMPACTION_OPTIONS))
            .grouped(2);

        for(Array<String> keyValue: groupedOptions){
            compactionStrategy = compactionStrategy.withOption(keyValue.get(0), keyValue.get(1));
        }

        return createTable.withCompaction(compactionStrategy);
    }

    private static CreateTableWithOptions gcGraceSeconds(final CreateTableWithOptions createTable,
                                                         final Configuration configuration) {
        if (!configuration.has(GC_GRACE_SECONDS)) {
            return createTable;
        }
        return createTable.withGcGraceSeconds(configuration.get(GC_GRACE_SECONDS));
    }

    private static CreateTableWithOptions speculativeRetryOptions(final CreateTableWithOptions createTable,
                                                                  final Configuration configuration) {
        if (!configuration.has(SPECULATIVE_RETRY)) {
            return createTable;
        }
        return createTable.withSpeculativeRetry(configuration.get(SPECULATIVE_RETRY));
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
        return cqlSliceFunction.getSlice(query, txh);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("The CQL backend does not support multi-key queries");
    }

    public static class CQLResultSetIterator implements RecordIterator<Tuple3<StaticBuffer, StaticBuffer, Row>> {

        private java.util.Iterator<Row> resultSetIterator;

        public CQLResultSetIterator(ResultSet rs) {
            resultSetIterator = rs.iterator();
        }

        @Override
        public boolean hasNext() {
            return resultSetIterator.hasNext();
        }

        @Override
        public Tuple3<StaticBuffer, StaticBuffer, Row> next() {
            Row nextRow = resultSetIterator.next();
            return nextRow == null
                ? null
                : Tuple.of(StaticArrayBuffer.of(nextRow.getByteBuffer(COLUMN_COLUMN_NAME)),
                           StaticArrayBuffer.of(nextRow.getByteBuffer(VALUE_COLUMN_NAME)), nextRow);
        }

        @Override
        public void close() throws IOException {
            // NOP
        }
    }

    public BatchableStatement<BoundStatement> deleteColumn(final StaticBuffer key, final StaticBuffer column) {
        return deleteColumn(key, column, null);
    }

    public BatchableStatement<BoundStatement> deleteColumn(final StaticBuffer key, final StaticBuffer column, final Long timestamp) {
        BoundStatementBuilder builder = deleteColumn.boundStatementBuilder()
            .setByteBuffer(KEY_BINDING, key.asByteBuffer())
            .setByteBuffer(COLUMN_BINDING, column.asByteBuffer());
        if (timestamp != null) {
            builder = builder.setLong(TIMESTAMP_BINDING, timestamp);
        }
        return builder.build();
    }

    public BatchableStatement<BoundStatement> insertColumn(final StaticBuffer key, final Entry entry) {
        return insertColumn(key, entry, null);
    }

    public BatchableStatement<BoundStatement> insertColumn(final StaticBuffer key, final Entry entry, final Long timestamp) {
        final Integer ttl = (Integer) entry.getMetaData().get(EntryMetaData.TTL);
        BoundStatementBuilder builder = ttl != null ? insertColumnWithTTL.boundStatementBuilder() : insertColumn.boundStatementBuilder();
        builder = builder.setByteBuffer(KEY_BINDING, key.asByteBuffer())
            .setByteBuffer(COLUMN_BINDING, entry.getColumn().asByteBuffer())
            .setByteBuffer(VALUE_BINDING, entry.getValue().asByteBuffer());
        if (ttl != null) {
            builder = builder.setInt(TTL_BINDING, ttl);
        }
        if (timestamp != null) {
            builder = builder.setLong(TIMESTAMP_BINDING, timestamp);
        }
        return builder.build();
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
            new CQLPagingIterator(
                getKeysRanged.boundStatementBuilder()
                    .setToken(KEY_START_BINDING, tokenMap.newToken(query.getKeyStart().asByteBuffer()))
                    .setToken(KEY_END_BINDING, tokenMap.newToken(query.getKeyEnd().asByteBuffer()))
                    .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                    .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                    .setPageSize(this.storeManager.getPageSize())
                    .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())))
            .getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        if (!this.storeManager.getFeatures().hasUnorderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when a random partitioner (md5 or murmur3) is used.");
        }

        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.getter,
                new CQLPagingIterator(
                    getKeysAll.boundStatementBuilder()
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setPageSize(this.storeManager.getPageSize())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    /**
     * This class provides a paging implementation that sits on top of the DSE Cassandra driver. The driver already
     * has its own built in paging support but this has limitations when doing a full scan of the key ring due
     * to how driver paging metadata is stored. The driver stores a full history of a given query's paging metadata
     * which can lead to OOM issues on non-trivially sized data sets. This class overcomes this by forcing the internal
     * metadata which isn't needed anymore to be deleted to be free for GC.
     */
    private class CQLPagingIterator implements Iterator<Row> {

        private final List<ExecutionInfo> dseStoredExecutionInfos;
        private final java.util.Iterator<Row> currentPageIterator;

        public CQLPagingIterator(BoundStatement boundStatement) {
            ResultSet currentResultSet = session.execute(boundStatement);
            currentPageIterator = currentResultSet.iterator();
            this.dseStoredExecutionInfos = currentResultSet.getExecutionInfos();
        }

        @Override
        public boolean hasNext() {
            return currentPageIterator.hasNext();
        }

        @Override
        public Row next() {
            if(dseStoredExecutionInfos.size()>1){
                ExecutionInfo lastExecutionInfo = dseStoredExecutionInfos.get(dseStoredExecutionInfos.size()-1);
                dseStoredExecutionInfos.clear();
                dseStoredExecutionInfos.add(lastExecutionInfo);
            }
            return currentPageIterator.next();
        }
    }
}
