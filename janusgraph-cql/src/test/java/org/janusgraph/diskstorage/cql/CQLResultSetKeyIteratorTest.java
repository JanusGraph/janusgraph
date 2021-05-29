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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Array;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CQLResultSetKeyIteratorTest {

    private static final SliceQuery ALL_COLUMNS = new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128));

    @Test
    public void testIterator() throws IOException {
        final Array<Row> rows = Array.rangeClosed(1, 100).map(idx -> {
            final Row row = mock(Row.class);
            when(row.getByteBuffer("key")).thenReturn(ByteBuffer.wrap(Integer.toString(idx / 5).getBytes()));
            when(row.getByteBuffer("column1")).thenReturn(ByteBuffer.wrap(Integer.toString(idx % 5).getBytes()));
            when(row.getByteBuffer("value")).thenReturn(ByteBuffer.wrap(Integer.toString(idx).getBytes()));
            return row;
        });

        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(rows.iterator());

        final CQLColValGetter getter = new CQLColValGetter(new EntryMetaData[0]);
        try (final CQLResultSetKeyIterator resultSetKeyIterator = new CQLResultSetKeyIterator(ALL_COLUMNS, getter, resultSet)) {
            int i = 0;
            while (resultSetKeyIterator.hasNext()) {
                final StaticBuffer next = resultSetKeyIterator.next();

                final RecordIterator<Entry> entries = resultSetKeyIterator.getEntries();
                while (entries.hasNext()) {
                    final Row row = rows.get(i++);
                    final Entry entry = entries.next();

                    assertEquals(row.getByteBuffer("key"), next.asByteBuffer());
                    assertEquals(row.getByteBuffer("column1"), entry.getColumn().asByteBuffer());
                    assertEquals(row.getByteBuffer("value"), entry.getValue().asByteBuffer());
                }
            }
        }
    }

    @Test
    public void testEmpty() throws IOException {
        final Array<Row> rows = Array.empty();

        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(rows.iterator());

        final CQLColValGetter getter = new CQLColValGetter(new EntryMetaData[0]);
        try (final CQLResultSetKeyIterator resultSetKeyIterator = new CQLResultSetKeyIterator(ALL_COLUMNS, getter, resultSet)) {
            assertFalse(resultSetKeyIterator.hasNext());
        }
    }

    @Test
    public void testUneven() throws IOException {
        final Array<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> keysMap = generateRandomKeysMap();
        final ResultSet resultSet = generateMockedResultSet(keysMap);

        final CQLColValGetter getter = new CQLColValGetter(new EntryMetaData[0]);
        try (final CQLResultSetKeyIterator resultSetKeyIterator = new CQLResultSetKeyIterator(ALL_COLUMNS, getter, resultSet)) {
            final Iterator<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> iterator = keysMap.iterator();
            while (resultSetKeyIterator.hasNext()) {
                final StaticBuffer next = resultSetKeyIterator.next();
                try (final RecordIterator<Entry> entries = resultSetKeyIterator.getEntries()) {
                    final Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>> current = iterator.next();
                    final ByteBuffer currentKey = current._1;
                    final Array<Tuple2<ByteBuffer, ByteBuffer>> columnValues = current._2;

                    final Iterator<Tuple2<ByteBuffer, ByteBuffer>> columnIterator = columnValues.iterator();
                    while (entries.hasNext()) {
                        final Entry entry = entries.next();
                        final Tuple2<ByteBuffer, ByteBuffer> columnAndValue = columnIterator.next();

                        assertEquals(currentKey, next.asByteBuffer());
                        assertEquals(columnAndValue._1, entry.getColumn().asByteBuffer());
                        assertEquals(columnAndValue._2, entry.getValue().asByteBuffer());
                        assertEquals(columnIterator.hasNext(), entries.hasNext());
                    }
                }
            }
        }
    }

    @Test
    public void testPartialIterateColumns() throws IOException {
        final Random random = new Random();
        final Array<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> keysMap = generateRandomKeysMap();
        final ResultSet resultSet = generateMockedResultSet(keysMap);

        final CQLColValGetter getter = new CQLColValGetter(new EntryMetaData[0]);
        try (final CQLResultSetKeyIterator resultSetKeyIterator = new CQLResultSetKeyIterator(ALL_COLUMNS, getter, resultSet)) {
            final Iterator<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> iterator = keysMap.iterator();
            while (resultSetKeyIterator.hasNext()) {
                final StaticBuffer next = resultSetKeyIterator.next();
                try (final RecordIterator<Entry> entries = resultSetKeyIterator.getEntries()) {
                    final Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>> current = iterator.next();
                    final ByteBuffer currentKey = current._1;
                    final Array<Tuple2<ByteBuffer, ByteBuffer>> columnValues = current._2;

                    final Iterator<Tuple2<ByteBuffer, ByteBuffer>> columnIterator = columnValues.iterator();
                    while (entries.hasNext()) {
                        final Entry entry = entries.next();
                        final Tuple2<ByteBuffer, ByteBuffer> columnAndValue = columnIterator.next();

                        assertEquals(currentKey, next.asByteBuffer());
                        assertEquals(columnAndValue._1, entry.getColumn().asByteBuffer());
                        assertEquals(columnAndValue._2, entry.getValue().asByteBuffer());
                        assertEquals(columnIterator.hasNext(), entries.hasNext());

                        // 10% of the time, don't complete the iteration
                        if (random.nextInt(10) == 0) {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testNoIterateColumns() throws IOException {
        final Array<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> keysMap = generateRandomKeysMap();
        final ResultSet resultSet = generateMockedResultSet(keysMap);

        final CQLColValGetter getter = new CQLColValGetter(new EntryMetaData[0]);
        try (final CQLResultSetKeyIterator resultSetKeyIterator = new CQLResultSetKeyIterator(ALL_COLUMNS, getter, resultSet)) {
            final Iterator<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> iterator = keysMap.iterator();
            while (resultSetKeyIterator.hasNext()) {
                final StaticBuffer next = resultSetKeyIterator.next();
                assertEquals(iterator.next()._1, next.asByteBuffer());
            }
        }
    }

    private Array<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> generateRandomKeysMap(){
        final Random random = new Random();

        final Function1<Integer, ByteBuffer> randomLong = idx -> {
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(random.nextLong());
            buffer.flip();
            return buffer;
        };

        return Array.range(0, random.nextInt(100) + 100)
            .map(randomLong)
            .map(key -> Tuple.of(key, Array.rangeClosed(0, random.nextInt(100) + 1)
                .map(idx -> Tuple.of(randomLong.apply(idx), randomLong.apply(idx)))));
    }

    private ResultSet generateMockedResultSet(Array<Tuple2<ByteBuffer, Array<Tuple2<ByteBuffer, ByteBuffer>>>> keysMap){
        final Seq<Row> rows = keysMap.flatMap(tuple -> tuple._2.map(columnAndValue -> {
            final Row row = mock(Row.class);
            when(row.getByteBuffer("key")).thenReturn(tuple._1);
            when(row.getByteBuffer("column1")).thenReturn(columnAndValue._1);
            when(row.getByteBuffer("value")).thenReturn(columnAndValue._2);
            return row;
        }));

        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(rows.iterator());

        return resultSet;
    }
}
