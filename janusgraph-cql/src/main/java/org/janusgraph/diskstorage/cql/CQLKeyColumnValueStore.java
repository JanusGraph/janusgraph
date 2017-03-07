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

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.*;
import static javaslang.API.*;
import static javaslang.Predicates.instanceOf;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import javaslang.Lazy;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.Tuple3;
import javaslang.collection.Array;
import javaslang.collection.Iterator;
import javaslang.concurrent.Future;
import javaslang.control.Try;

public class CQLKeyColumnValueStore implements KeyColumnValueStore {

    private static final String KEY_COLUMN_NAME = "key";
    private static final String COLUMN_COLUMN_NAME = "column1";
    private static final String VALUE_COLUMN_NAME = "value";
    private static final String WRITETIME_COLUMN_NAME = "writetime";
    private static final String TTL_COLUMN_NAME = "ttl";

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
            Case(instanceOf(QueryValidationException.class), qve -> new PermanentBackendException(qve)),
            Case(instanceOf(UnsupportedFeatureException.class), ufe -> new PermanentBackendException(ufe)),
            Case($(), t -> new TemporaryBackendException(t)));

    private final CQLStoreManager storeManager;
    private final Session session;
    private final String tableName;
    private final Getter getter;
    private final Runnable closer;

    private final PreparedStatement getSlice;
    private final PreparedStatement getKeysAll;
    private final PreparedStatement getKeysRanged;
    private final PreparedStatement deleteColumn;
    private final PreparedStatement insertColumn;
    private final PreparedStatement insertColumnWithTTL;

    public CQLKeyColumnValueStore(final CQLStoreManager storeManager, final String tableName, final Configuration configuration, final Runnable closer) {
        this.storeManager = storeManager;
        this.closer = closer;
        this.session = this.storeManager.getSession();
        this.tableName = tableName;
        this.getter = new Getter(storeManager.getMetaDataSchema(this.tableName));

        initialiseTable(this.session, this.storeManager.getKeyspaceName(), tableName, configuration);

        // @formatter:off
        this.getSlice = this.session.prepare(select()
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .fcall("writetime", column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .fcall("ttl", column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .from(this.storeManager.getKeyspaceName(), this.tableName)
                .where(eq(KEY_COLUMN_NAME, bindMarker(KEY_BINDING)))
                .and(gte(COLUMN_COLUMN_NAME, bindMarker(SLICE_START_BINDING)))
                .and(lt(COLUMN_COLUMN_NAME, bindMarker(SLICE_END_BINDING)))
                .limit(bindMarker(LIMIT_BINDING)));

        this.getKeysRanged = this.session.prepare(select()
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .fcall("writetime", column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .fcall("ttl", column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
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
                .fcall("writetime", column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .fcall("ttl", column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
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

    private static void initialiseTable(final Session session, final String keyspaceName, final String tableName, final Configuration configuration) {
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
                Case("LZ4Compressor", lz4()),
                Case("SnappyCompressor", snappy()),
                Case("DeflateCompressor", deflate()))
                .withChunkLengthInKb(configuration.get(CF_COMPRESSION_BLOCK_SIZE));
    }

    private static CompactionOptions<?> compactionOptions(final Configuration configuration) {
        if (!configuration.has(COMPACTION_STRATEGY)) {
            return null;
        }

        final CompactionOptions<?> compactionOptions = Match(configuration.get(COMPACTION_STRATEGY))
                .of(
                        Case("SizeTieredCompactionStrategy", sizedTieredStategy()),
                        Case("DateTieredCompactionStrategy", dateTieredStrategy()),
                        Case("LeveledCompactionStrategy", leveledStrategy()));
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
        final Future<EntryList> result = Future.fromJavaFuture(this.session.executeAsync(this.getSlice.bind()
                .setBytes(KEY_BINDING, query.getKey().asByteBuffer())
                .setBytes(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                .setBytes(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                .setInt(LIMIT_BINDING, query.getLimit())
                .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel())))
                .map(resultSet -> fromResultSet(resultSet, this.getter));
        result.await();
        return result.getValue().get().getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        final Future<Map<StaticBuffer, EntryList>> result = Future.sequence(Iterator.ofAll(keys)
                .<Future<Tuple2<StaticBuffer, ResultSet>>> map(key -> Future.fromJavaFuture(
                        this.session.executeAsync(this.getSlice.bind()
                                .setBytes(KEY_BINDING, key.asByteBuffer())
                                .setBytes(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                                .setBytes(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                                .setInt(LIMIT_BINDING, query.getLimit())
                                .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel())))
                        .map(future -> Tuple.of(key, future))))
                .map(sequence -> sequence.toJavaMap(pair -> Tuple.of(pair._1, fromResultSet(pair._2, this.getter))));
        result.await();
        return result.getValue().get().getOrElseThrow(EXCEPTION_MAPPER);
    }

    private static EntryList fromResultSet(final ResultSet resultSet, final GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> getter) {
        final Lazy<ArrayList<Row>> lazyList = Lazy.of(() -> Lists.newArrayList(resultSet));
        // Use the Iterable overload of ofByteBuffer as it's able to allocate the byte array up front.
        // To ensure that the Iterator instance is recreated, it is created within the closure otherwise
        // the same iterator would be reused and would be exhausted.
        return StaticArrayEntryList.ofStaticBuffer(() -> Iterator.ofAll(lazyList.get())
                .<Tuple3<StaticBuffer, StaticBuffer, Row>> map(row -> Tuple.of(
                        StaticArrayBuffer.of(row.getBytes(COLUMN_COLUMN_NAME)),
                        StaticArrayBuffer.of(row.getBytes(VALUE_COLUMN_NAME)),
                        row)),
                getter);
    }

    Statement deleteColumn(final StaticBuffer key, final StaticBuffer column, final long timestamp) {
        return this.deleteColumn.bind()
                .setBytes(KEY_BINDING, key.asByteBuffer())
                .setBytes(COLUMN_BINDING, column.asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

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
        return Try.of(() -> new ResultSetKeyIterator(
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

        return Try.of(() -> new ResultSetKeyIterator(
                query,
                this.getter,
                this.session.execute(this.getKeysAll.bind()
                        .setBytes(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setBytes(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setFetchSize(this.storeManager.getPageSize())
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }

    // ------------------------------------------------------------------------
    // Inner classes
    // ------------------------------------------------------------------------

    static class ResultSetKeyIterator extends AbstractIterator<StaticBuffer> implements KeyIterator {

        private final SliceQuery sliceQuery;
        private final Getter getter;
        private final Iterator<Row> iterator;

        private Row currentRow = null;
        private StaticBuffer currentKey = null;
        private StaticBuffer lastKey = null;

        ResultSetKeyIterator(final SliceQuery sliceQuery, final Getter getter, final ResultSet resultSet) {
            this.sliceQuery = sliceQuery;
            this.getter = getter;
            this.iterator = Iterator.ofAll(resultSet.iterator())
                    .peek(row -> {
                        this.currentRow = row;
                        this.currentKey = StaticArrayBuffer.of(row.getBytes(KEY_COLUMN_NAME));
                    });
        }

        @Override
        protected StaticBuffer computeNext() {
            if (this.currentKey != null && !this.currentKey.equals(this.lastKey)) {
                this.lastKey = this.currentKey;
                return this.lastKey;
            }

            while (this.iterator.hasNext()) {
                this.iterator.next();
                if (!this.currentKey.equals(this.lastKey)) {
                    this.lastKey = this.currentKey;
                    return this.lastKey;
                }
            }
            return endOfData();
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            return new EntryRecordIterator(this.sliceQuery, this.getter, Iterator.of(this.currentRow).concat(this.iterator), this.currentKey);
        }

        @Override
        public void close() throws IOException {
            // NOP
        }

        static class EntryRecordIterator extends AbstractIterator<Entry> implements RecordIterator<Entry> {

            private final Getter getter;
            private final Iterator<Tuple3<StaticBuffer, StaticBuffer, Row>> iterator;

            EntryRecordIterator(final SliceQuery sliceQuery, final Getter getter, final Iterator<Row> iterator, final StaticBuffer key) {
                this.getter = getter;
                final StaticBuffer sliceEnd = sliceQuery.getSliceEnd();
                this.iterator = iterator
                        .<Tuple3<StaticBuffer, StaticBuffer, Row>> map(row -> Tuple.of(
                                StaticArrayBuffer.of(row.getBytes(COLUMN_COLUMN_NAME)),
                                StaticArrayBuffer.of(row.getBytes(VALUE_COLUMN_NAME)),
                                row))
                        .takeWhile(tuple -> key.equals(StaticArrayBuffer.of(tuple._3.getBytes(KEY_COLUMN_NAME))) && !sliceEnd.equals(tuple._1))
                        .take(sliceQuery.getLimit());
            }

            @Override
            protected Entry computeNext() {
                if (this.iterator.hasNext()) {
                    return StaticArrayEntry.ofStaticBuffer(this.iterator.next(), this.getter);
                }
                return endOfData();
            }

            @Override
            public void close() throws IOException {
                // NOP
            }
        }
    }

    static class Getter implements GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> {

        private final EntryMetaData[] schema;

        Getter(final EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public StaticBuffer getColumn(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
            return tuple._1;
        }

        @Override
        public StaticBuffer getValue(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
            return tuple._2;
        }

        @Override
        public EntryMetaData[] getMetaSchema(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
            return this.schema;
        }

        @Override
        public Object getMetaData(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple, final EntryMetaData metaData) {
            switch (metaData) {
                case TIMESTAMP:
                    return tuple._3.getLong(WRITETIME_COLUMN_NAME);
                case TTL:
                    return tuple._3.getInt(TTL_COLUMN_NAME);
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + metaData);
            }
        }
    }
}
