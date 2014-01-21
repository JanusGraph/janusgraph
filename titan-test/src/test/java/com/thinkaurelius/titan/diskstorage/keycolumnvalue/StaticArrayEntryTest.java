package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntryTest {

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

        rb = entry.getColumnAs(StaticBuffer.STATIC_FACTORY).asReadBuffer();
        for (int i=1;i<=4;i++) assertEquals(i,rb.getInt());
        assertFalse(rb.hasRemaining());
        rb = entry.getValueAs(StaticBuffer.STATIC_FACTORY).asReadBuffer();
        for (int i=5;i<=6;i++) assertEquals(i,rb.getInt());
        assertFalse(rb.hasRemaining());
    }

    @Test
    public void testInversion() {
        WriteBuffer wb = new WriteByteBuffer(20);
        wb.putInt(1).putInt(2).putInt(3).putInt(4);
        Entry entry = new StaticArrayEntry(wb.getStaticBufferFlipBytes(4,2*4),3*4);
        ReadBuffer rb = entry.asReadBuffer();
        assertEquals(1,rb.getInt());
        rb.invert();
        assertEquals(2,rb.getInt());
        rb.invert();
        assertEquals(3,rb.getInt());
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
                num++;
            }
            assertEquals(entries.size(),num);
            Iterator<Entry> iter = el[i].reuseIterator();
            num=0;
            while (iter.hasNext()) {
                checkEntry(iter.next(), entries);
                num++;
            }
            assertEquals(entries.size(),num);
        }


    }

    private static enum BBEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, ByteBuffer> {

        INSTANCE;

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
    }

    private static enum ByteEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, byte[]> {

        INSTANCE;

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
    }

    private static enum StaticEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<Integer, Long>, StaticBuffer> {

        INSTANCE;

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

}
