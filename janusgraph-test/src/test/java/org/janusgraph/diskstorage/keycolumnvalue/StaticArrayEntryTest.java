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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.relations.RelationCache;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntryTest {

    private static final RelationCache cache = new RelationCache(Direction.OUT,5,105,"Hello");

    private static final EntryMetaData[] metaSchema = { EntryMetaData.TIMESTAMP, EntryMetaData.TTL, EntryMetaData.VISIBILITY};
    private static final Map<EntryMetaData,Object> metaData = new EntryMetaData.Map() {{
        put(EntryMetaData.TIMESTAMP,Long.valueOf(101));
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
        assertTrue(Arrays.equals(new byte[]{2,3},r.getBytes(2)));
        assertEquals(1,r.getShort());
        assertTrue(Arrays.equals(new short[]{2,3},r.getShorts(2)));
        assertEquals(1,r.getInt());
        assertEquals(2,r.getInt());
        assertTrue(Arrays.equals(new int[]{3},r.getInts(1)));
        assertEquals(1,r.getLong());
        assertTrue(Arrays.equals(new long[]{2,3},r.getLongs(2)));
        assertEquals(1.0,r.getFloat(),0.00001);
        assertTrue(Arrays.equals(new float[]{2.0f,3.0f},r.getFloats(2)));
        assertEquals(1,r.getDouble(),0.0001);
        assertTrue(Arrays.equals(new double[]{2.0,3.0},r.getDoubles(2)));
        assertEquals((char)101,r.getChar());
        assertEquals((char)102,r.getChar());
        assertTrue(Arrays.equals(new char[]{(char)103},r.getChars(1)));

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
        Map<Integer,Long> entries = new HashMap<Integer,Long>();
        for (int i=0;i<50;i++) entries.put(i*2+7,Math.round(Math.random()/2*Long.MAX_VALUE));

        EntryList[] el = new EntryList[7];
        el[0] = StaticArrayEntryList.ofBytes(entries.entrySet(), ByteEntryGetter.INSTANCE);

        el[1] = StaticArrayEntryList.ofByteBuffer(entries.entrySet(), BBEntryGetter.INSTANCE);

        el[2] = StaticArrayEntryList.ofStaticBuffer(entries.entrySet(), StaticEntryGetter.INSTANCE);

        el[3] = StaticArrayEntryList.ofByteBuffer(entries.entrySet().iterator(), BBEntryGetter.INSTANCE);

        el[4] = StaticArrayEntryList.ofStaticBuffer(entries.entrySet().iterator(), StaticEntryGetter.INSTANCE);

        el[5] = StaticArrayEntryList.of(Iterables.transform(entries.entrySet(),new Function<Map.Entry<Integer, Long>, Entry>() {
            @Nullable
            @Override
            public Entry apply(@Nullable Map.Entry<Integer, Long> entry) {
                return StaticArrayEntry.ofByteBuffer(entry, BBEntryGetter.INSTANCE);
            }
        }));

        el[6] = StaticArrayEntryList.of(Iterables.transform(entries.entrySet(),new Function<Map.Entry<Integer, Long>, Entry>() {
            @Nullable
            @Override
            public Entry apply(@Nullable Map.Entry<Integer, Long> entry) {
                return StaticArrayEntry.ofBytes(entry, ByteEntryGetter.INSTANCE);
            }
        }));

        for (int i = 0; i < el.length; i++) {
            assertEquals(entries.size(),el[i].size());
            int num=0;
            for (Entry e : el[i]) {
                checkEntry(e,entries);
                assertFalse(e.hasMetaData());
                assertTrue(e.getMetaData().isEmpty());
                assertNull(e.getCache());
                e.setCache(cache);
                num++;
            }
            assertEquals(entries.size(),num);
            Iterator<Entry> iter = el[i].reuseIterator();
            num=0;
            while (iter.hasNext()) {
                Entry e = iter.next();
                checkEntry(e, entries);
                assertFalse(e.hasMetaData());
                assertTrue(e.getMetaData().isEmpty());
                assertEquals(cache,e.getCache());
                num++;
            }
            assertEquals(entries.size(),num);
        }
    }

    /**
     * Copied from above - the only difference is using schema instances and checking the schema
     */
    @Test
    public void testEntryListWithMetaSchema() {
        Map<Integer,Long> entries = new HashMap<Integer,Long>();
        for (int i=0;i<50;i++) entries.put(i*2+7,Math.round(Math.random()/2*Long.MAX_VALUE));

        EntryList[] el = new EntryList[7];
        el[0] = StaticArrayEntryList.ofBytes(entries.entrySet(), ByteEntryGetter.SCHEMA_INSTANCE);

        el[1] = StaticArrayEntryList.ofByteBuffer(entries.entrySet(), BBEntryGetter.SCHEMA_INSTANCE);

        el[2] = StaticArrayEntryList.ofStaticBuffer(entries.entrySet(), StaticEntryGetter.SCHEMA_INSTANCE);

        el[3] = StaticArrayEntryList.ofByteBuffer(entries.entrySet().iterator(), BBEntryGetter.SCHEMA_INSTANCE);

        el[4] = StaticArrayEntryList.ofStaticBuffer(entries.entrySet().iterator(), StaticEntryGetter.SCHEMA_INSTANCE);

        el[5] = StaticArrayEntryList.of(Iterables.transform(entries.entrySet(),new Function<Map.Entry<Integer, Long>, Entry>() {
            @Nullable
            @Override
            public Entry apply(@Nullable Map.Entry<Integer, Long> entry) {
                return StaticArrayEntry.ofByteBuffer(entry, BBEntryGetter.SCHEMA_INSTANCE);
            }
        }));

        el[6] = StaticArrayEntryList.of(Iterables.transform(entries.entrySet(),new Function<Map.Entry<Integer, Long>, Entry>() {
            @Nullable
            @Override
            public Entry apply(@Nullable Map.Entry<Integer, Long> entry) {
                return StaticArrayEntry.ofBytes(entry, ByteEntryGetter.SCHEMA_INSTANCE);
            }
        }));

        for (int i = 0; i < el.length; i++) {
            //System.out.println("Iteration: " + i);
            assertEquals(entries.size(),el[i].size());
            int num=0;
            for (Entry e : el[i]) {
                checkEntry(e,entries);
                assertTrue(e.hasMetaData());
                assertFalse(e.getMetaData().isEmpty());
                assertEquals(metaData,e.getMetaData());
                assertNull(e.getCache());
                e.setCache(cache);
                num++;
            }
            assertEquals(entries.size(),num);
            Iterator<Entry> iter = el[i].reuseIterator();
            num=0;
            while (iter.hasNext()) {
                Entry e = iter.next();
                assertTrue(e.hasMetaData());
                assertFalse(e.getMetaData().isEmpty());
                assertEquals(metaData,e.getMetaData());
                assertEquals(cache,e.getCache());
                checkEntry(e, entries);
                num++;
            }
            assertEquals(entries.size(),num);
        }
    }

    @Test
    public void testTTLMetadata() throws Exception {
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

    private static enum BBEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, ByteBuffer> {

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

    private static enum ByteEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, byte[]> {

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

    private static enum StaticEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, StaticBuffer> {

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
