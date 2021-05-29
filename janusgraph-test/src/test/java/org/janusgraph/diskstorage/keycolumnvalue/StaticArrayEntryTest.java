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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.relations.RelationCache;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntryTest {

    private static final RelationCache cache = new RelationCache(Direction.OUT,5,105,"Hello");

    private static final EntryMetaData[] metaSchema = {
        EntryMetaData.TIMESTAMP, EntryMetaData.TTL, EntryMetaData.VISIBILITY
    };
    private static final Map<EntryMetaData,Object> metaData = new EntryMetaData.Map() {{
        put(EntryMetaData.TIMESTAMP, 101L);
        put(EntryMetaData.TTL, 42);
        put(EntryMetaData.VISIBILITY,"SOS/K5a-89 SOS/sdf3");
    }};

    @Test
    public void testArrayBuffer() {
        WriteBuffer wb = new WriteByteBuffer(128);
        wb.putInt(1).putInt(2).putInt(3).putInt(4);
        int valuePos = wb.getPosition();
        wb.putInt(5).putInt(6);
        Entry entry = new StaticArrayEntry(wb.getStaticBuffer(),valuePos);
        assertEquals(4*4,entry.getValuePosition());
        assertEquals(6*4,entry.length());
        assertTrue(entry.hasValue());
        for (int i=1;i<=6;i++) assertEquals(i,entry.getInt((i-1)*4));
        ReadBuffer rb = entry.asReadBuffer();
        for (int i=1;i<=6;i++) assertEquals(i,rb.getInt());
        assertFalse(rb.hasRemaining());

        assertNull(entry.getCache());
        entry.setCache(cache);
        assertEquals(cache,entry.getCache());

        rb = entry.getColumnAs(StaticBuffer.STATIC_FACTORY).asReadBuffer();
        for (int i=1;i<=4;i++) assertEquals(i,rb.getInt());
        assertFalse(rb.hasRemaining());
        rb = entry.getValueAs(StaticBuffer.STATIC_FACTORY).asReadBuffer();
        for (int i=5;i<=6;i++) assertEquals(i,rb.getInt());
        assertFalse(rb.hasRemaining());
    }

    @Test
    public void testReadWrite() {
        WriteBuffer b = new WriteByteBuffer(10);
        for (int i=1;i<4;i++) b.putByte((byte) i);
        for (int i=1;i<4;i++) b.putShort((short) i);
        for (int i=1;i<4;i++) b.putInt(i);
        for (int i=1;i<4;i++) b.putLong(i);
        for (int i=1;i<4;i++) b.putFloat(i);
        for (int i=1;i<4;i++) b.putDouble(i);
        for (int i=101;i<104;i++) b.putChar((char) i);

        ReadBuffer r = b.getStaticBuffer().asReadBuffer();
        assertEquals(1,r.getByte());
        assertArrayEquals(new byte[]{2, 3}, r.getBytes(2));
        assertEquals(1,r.getShort());
        assertArrayEquals(new short[]{2, 3}, r.getShorts(2));
        assertEquals(1,r.getInt());
        assertEquals(2,r.getInt());
        assertArrayEquals(new int[]{3}, r.getInts(1));
        assertEquals(1,r.getLong());
        assertArrayEquals(new long[]{2, 3}, r.getLongs(2));
        assertEquals(1.0,r.getFloat(),0.00001);
        assertArrayEquals(new float[]{2.0f, 3.0f}, r.getFloats(2));
        assertEquals(1,r.getDouble(),0.0001);
        assertArrayEquals(new double[]{2.0, 3.0}, r.getDoubles(2));
        assertEquals((char)101,r.getChar());
        assertEquals((char)102,r.getChar());
        assertArrayEquals(new char[]{(char) 103}, r.getChars(1));

    }


    @Test
    public void testInversion() {
        WriteBuffer wb = new WriteByteBuffer(20);
        wb.putInt(1).putInt(2).putInt(3).putInt(4);
        Entry entry = new StaticArrayEntry(wb.getStaticBufferFlipBytes(4,2*4),3*4);
        ReadBuffer rb = entry.asReadBuffer();
        assertEquals(1, rb.getInt());
        assertEquals(2,rb.subrange(4,true).getInt());
        assertEquals(~2, rb.getInt());
        assertEquals(3, rb.getInt());
        assertEquals(4,rb.getInt());
        rb.movePositionTo(entry.getValuePosition());
        assertEquals(4,rb.getInt());
    }

    @Test
    public void testEntryList() {

        final Map<Integer,Long> entries = generateRandomEntries();
        EntryList[] el = generateEntryListArray(entries, "INSTANCE");

        for (final EntryList anEl : el) {
            assertEquals(entries.size(), anEl.size());
            int num = 0;
            for (final Entry e : anEl) {
                checkEntry(e, entries);
                assertFalse(e.hasMetaData());
                assertTrue(e.getMetaData().isEmpty());
                assertNull(e.getCache());
                e.setCache(cache);
                num++;
            }
            assertEquals(entries.size(), num);
            final Iterator<Entry> iterator = anEl.reuseIterator();
            num = 0;
            while (iterator.hasNext()) {
                final Entry e = iterator.next();
                checkEntry(e, entries);
                assertFalse(e.hasMetaData());
                assertTrue(e.getMetaData().isEmpty());
                assertEquals(cache, e.getCache());
                num++;
            }
            assertEquals(entries.size(), num);
        }
    }

    /**
     * Copied from above - the only difference is using schema instances and checking the schema
     */
    @Test
    public void testEntryListWithMetaSchema() {

        final Map<Integer,Long> entries = generateRandomEntries();
        EntryList[] el = generateEntryListArray(entries, "SCHEMA_INSTANCE");

        for (final EntryList anEl : el) {
            //System.out.println("Iteration: " + i);
            assertEquals(entries.size(), anEl.size());
            int num = 0;
            for (final Entry e : anEl) {
                checkEntry(e, entries);
                assertTrue(e.hasMetaData());
                assertFalse(e.getMetaData().isEmpty());
                assertEquals(metaData, e.getMetaData());
                assertNull(e.getCache());
                e.setCache(cache);
                num++;
            }
            assertEquals(entries.size(), num);
            final Iterator<Entry> iter = anEl.reuseIterator();
            num = 0;
            while (iter.hasNext()) {
                final Entry e = iter.next();
                assertTrue(e.hasMetaData());
                assertFalse(e.getMetaData().isEmpty());
                assertEquals(metaData, e.getMetaData());
                assertEquals(cache, e.getCache());
                checkEntry(e, entries);
                num++;
            }
            assertEquals(entries.size(), num);
        }
    }

    @Test
    public void testTTLMetadata() {
        WriteBuffer wb = new WriteByteBuffer(128);
        wb.putInt(1).putInt(2).putInt(3).putInt(4);
        int valuePos = wb.getPosition();
        wb.putInt(5).putInt(6);
        StaticArrayEntry entry = new StaticArrayEntry(wb.getStaticBuffer(),valuePos);
        entry.setMetaData(EntryMetaData.TTL, 42);
        assertEquals(42, entry.getMetaData().get(EntryMetaData.TTL));
    }

    private static void checkEntry(Entry e, Map<Integer,Long> entries) {
        ReadBuffer rb = e.asReadBuffer();
        int key = rb.getInt();
        assertEquals(e.getValuePosition(),rb.getPosition());
        assertTrue(e.hasValue());
        long value = rb.getLong();
        assertFalse(rb.hasRemaining());
        assertEquals((long)entries.get(key),value);

        rb = e.getColumnAs(StaticBuffer.STATIC_FACTORY).asReadBuffer();
        assertEquals(key,rb.getInt());
        assertFalse(rb.hasRemaining());

        rb = e.getValueAs(StaticBuffer.STATIC_FACTORY).asReadBuffer();
        assertEquals(value,rb.getLong());
        assertFalse(rb.hasRemaining());

    }

    private Map<Integer, Long> generateRandomEntries(){

        final Map<Integer,Long> entries = new HashMap<>();

        for (int i=0;i<50;i++) {
            entries.put(i*2+7,Math.round(Math.random()/2*Long.MAX_VALUE));
        }

        return entries;
    }

    private EntryList[] generateEntryListArray(Map<Integer,Long> entries, String getterName){

        EntryList[] el = new EntryList[7];
        ByteEntryGetter byteEntryGetter = ByteEntryGetter.valueOf(getterName);
        BBEntryGetter bbEntryGetter = BBEntryGetter.valueOf(getterName);
        StaticEntryGetter staticEntryGetter = StaticEntryGetter.valueOf(getterName);

        el[0] = StaticArrayEntryList.ofBytes(entries.entrySet(), byteEntryGetter);

        el[1] = StaticArrayEntryList.ofByteBuffer(entries.entrySet(), bbEntryGetter);

        el[2] = StaticArrayEntryList.ofStaticBuffer(entries.entrySet(), staticEntryGetter);

        el[3] = StaticArrayEntryList.ofByteBuffer(entries.entrySet().iterator(), bbEntryGetter);

        el[4] = StaticArrayEntryList.ofStaticBuffer(entries.entrySet().iterator(), staticEntryGetter);

        el[5] = StaticArrayEntryList.of(entries.entrySet().stream()
            .map(entry -> StaticArrayEntry.ofByteBuffer(entry, bbEntryGetter))
            .collect(Collectors.toList()));

        el[6] = StaticArrayEntryList.of(entries.entrySet().stream()
            .map(entry -> StaticArrayEntry.ofBytes(entry, byteEntryGetter))
            .collect(Collectors.toList()));

        return el;
    }

    private enum BBEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, ByteBuffer> {

        INSTANCE, SCHEMA_INSTANCE;

        @Override
        public ByteBuffer getColumn(Map.Entry<Integer, Long> element) {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(element.getKey()).flip();
            return b;
        }

        @Override
        public ByteBuffer getValue(Map.Entry<Integer, Long> element) {
            ByteBuffer b = ByteBuffer.allocate(8);
            b.putLong(element.getValue()).flip();
            return b;
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<Integer, Long> element) {
            if (this==INSTANCE) return StaticArrayEntry.EMPTY_SCHEMA;
            else return metaSchema;
        }

        @Override
        public Object getMetaData(Map.Entry<Integer, Long> element, EntryMetaData meta) {
            if (this==INSTANCE) throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            else return metaData.get(meta);
        }
    }

    private enum ByteEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, byte[]> {

        INSTANCE, SCHEMA_INSTANCE;

        @Override
        public byte[] getColumn(Map.Entry<Integer, Long> element) {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(element.getKey());
            return b.array();
        }

        @Override
        public byte[] getValue(Map.Entry<Integer, Long> element) {
            ByteBuffer b = ByteBuffer.allocate(8);
            b.putLong(element.getValue());
            return b.array();
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<Integer, Long> element) {
            if (this==INSTANCE) return StaticArrayEntry.EMPTY_SCHEMA;
            else return metaSchema;
        }

        @Override
        public Object getMetaData(Map.Entry<Integer, Long> element, EntryMetaData meta) {
            if (this==INSTANCE) throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            else return metaData.get(meta);
        }
    }

    private enum StaticEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, StaticBuffer> {

        INSTANCE, SCHEMA_INSTANCE;

        @Override
        public StaticBuffer getColumn(Map.Entry<Integer, Long> element) {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(element.getKey());
            return StaticArrayBuffer.of(b.array());
        }

        @Override
        public StaticBuffer getValue(Map.Entry<Integer, Long> element) {
            ByteBuffer b = ByteBuffer.allocate(8);
            b.putLong(element.getValue());
            return StaticArrayBuffer.of(b.array());
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<Integer, Long> element) {
            if (this==INSTANCE) return StaticArrayEntry.EMPTY_SCHEMA;
            else return metaSchema;
        }

        @Override
        public Object getMetaData(Map.Entry<Integer, Long> element, EntryMetaData meta) {
            if (this==INSTANCE) throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            else return metaData.get(meta);
        }
    }


}
