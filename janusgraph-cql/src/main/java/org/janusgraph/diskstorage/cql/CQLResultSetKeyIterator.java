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

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.AbstractIterator;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.Iterator;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.io.IOException;

/**
 * {@link SliceQuery} iterator that handles CQL result sets that may have more
 * data returned in each column than the {@link SliceQuery} has configured as
 * it's limit. I.e. the iterator only returns the number of entries for each Key
 * to the number of Columns specified in the {@link SliceQuery}s limit.
 */
class CQLResultSetKeyIterator extends AbstractIterator<StaticBuffer> implements KeyIterator {

    private final SliceQuery sliceQuery;
    private final CQLColValGetter getter;
    private final Iterator<Row> iterator;

    private Row currentRow = null;
    private StaticBuffer currentKey = null;
    private StaticBuffer lastKey = null;

    CQLResultSetKeyIterator(final SliceQuery sliceQuery, final CQLColValGetter getter, final Iterable<Row> resultSet) {
        this.sliceQuery = sliceQuery;
        this.getter = getter;
        this.iterator = Iterator.ofAll(resultSet)
                .peek(row -> {
                    this.currentRow = row;
                    this.currentKey = StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.KEY_COLUMN_NAME));
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
            if (this.currentKey != null && !this.currentKey.equals(this.lastKey)) {
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

        private final CQLColValGetter getter;
        private final Iterator<Tuple3<StaticBuffer, StaticBuffer, Row>> iterator;

        EntryRecordIterator(final SliceQuery sliceQuery, final CQLColValGetter getter, final Iterator<Row> iterator, final StaticBuffer key) {
            this.getter = getter;
            final StaticBuffer sliceEnd = sliceQuery.getSliceEnd();
            this.iterator = iterator
                    .<Tuple3<StaticBuffer, StaticBuffer, Row>> map(row -> Tuple.of(
                            StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.COLUMN_COLUMN_NAME)),
                            StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.VALUE_COLUMN_NAME)),
                            row))
                    .takeWhile(tuple -> key.equals(StaticArrayBuffer.of(tuple._3.getByteBuffer(CQLKeyColumnValueStore.KEY_COLUMN_NAME))) && !sliceEnd.equals(tuple._1))
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
