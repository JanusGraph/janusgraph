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

package org.janusgraph.diskstorage.util;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.*;
import org.janusgraph.graphdb.database.serialize.attribute.IntegerSerializer;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.util.encoding.StringEncoding;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntryList extends AbstractList<Entry> implements EntryList {

    /**
     * All of the entries are stored sequentially in this byte array. The limitAndValuePos array contains the offset and
     * value position information needed to re-construct individual entries from this array
     */
    private final byte[] data;
    /**
     * Stores the offset and value position information for each entry. The length of this array equals the number
     * of entries in this list. Hence, limitAndValuePos[i] contains the limit and value position information of entry i.
     * The first 32 bits of that long value are the offset (as int) and the second 32 bits are the value position (as int).
     */
    private final long[] limitAndValuePos;

    // ---- Transient fields ----
    private final RelationCache[] caches;
    private final EntryMetaData[] metaDataSchema;

    private StaticArrayEntryList(final byte[] data, final long[] limitAndValuePos, final EntryMetaData[] metaDataSchema) {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(limitAndValuePos!=null && limitAndValuePos.length>0);
        Preconditions.checkArgument(metaDataSchema!=null);
        this.data=data;
        this.limitAndValuePos=limitAndValuePos;
        this.caches = new RelationCache[limitAndValuePos.length];
        if (metaDataSchema.length==0) this.metaDataSchema = StaticArrayEntry.EMPTY_SCHEMA;
        else this.metaDataSchema = metaDataSchema;
    }

    private static int getLimit(long limitAndValuePos) {
        return (int)(limitAndValuePos>>>32l);
    }

    private static int getValuePos(long limitAndValuePos) {
        return (int)(limitAndValuePos&Integer.MAX_VALUE);
    }

    private static long getOffsetandValue(long offset, long valuePos) {
        assert valuePos>0;
        return (offset<<32l) + valuePos;
    }

    private boolean hasMetaData() {
        return metaDataSchema.length>0;
    }

    @Override
    public Entry get(int index) {
        Preconditions.checkPositionIndex(index,limitAndValuePos.length);
        int offset = index>0?getLimit(limitAndValuePos[index-1]):0;
        Map<EntryMetaData,Object> metadata = EntryMetaData.EMPTY_METADATA;
        if (hasMetaData()) {
            metadata = new EntryMetaData.Map();
            offset = parseMetaData(metadata,offset);
        }
        return new StaticEntry(index, offset, getLimit(limitAndValuePos[index]), getValuePos(limitAndValuePos[index]),metadata);
    }

    @Override
    public int size() {
        return limitAndValuePos.length;
    }

    @Override
    public int getByteSize() {
        return  16 + 3*8 // object
                + data.length + 16 // data
                + limitAndValuePos.length*8 + 16 // limitAndValuePos;
                + caches.length*(40) + 16; // caches
    }

    private class StaticEntry extends BaseStaticArrayEntry {

        private final int index;
        private final Map<EntryMetaData,Object> metadata;

        public StaticEntry(final int index, final int offset, final int limit, final int valuePos,
                           final Map<EntryMetaData,Object> metadata) {
            super(data, offset, limit, valuePos);
            this.index=index;
            this.metadata=metadata;
        }

        @Override
        public boolean hasMetaData() {
            return !metadata.isEmpty();
        }

        @Override
        public Map<EntryMetaData,Object> getMetaData() {
            return metadata;
        }

        @Override
        public RelationCache getCache() {
            return caches[index];
        }

        @Override
        public void setCache(RelationCache cache) {
            Preconditions.checkNotNull(cache);
            caches[index] = cache;
        }

    }

    @Override
    public Iterator<Entry> reuseIterator() {
        return new SwappingEntry();
    }

    private class SwappingEntry extends ReadArrayBuffer implements Entry, Iterator<Entry> {

        private int currentIndex=-1;
        private int currentValuePos=-1;
        private Map<EntryMetaData,Object> metadata=null;

        public SwappingEntry() {
            super(data,0,0);
        }

        private void verifyAccess() {
            Preconditions.checkArgument(currentIndex>=0,"Illegal iterator access");
        }

        @Override
        public int getValuePosition() {
            verifyAccess();
            return currentValuePos;
        }

        @Override
        public ReadBuffer asReadBuffer() {
            super.movePositionTo(0);
            return this;
        }

        @Override
        public RelationCache getCache() {
            verifyAccess();
            return caches[currentIndex];
        }

        @Override
        public void setCache(RelationCache cache) {
            verifyAccess();
            caches[currentIndex]=cache;
        }

        public boolean hasMetaData() {
            verifyAccess();
            return !metadata.isEmpty();
        }

        @Override
        public Map<EntryMetaData,Object> getMetaData() {
            verifyAccess();
            return metadata;
        }

        //####### COPIED FROM StaticArrayEntry

        @Override
        public boolean hasValue() {
            return currentValuePos<length();
        }

        @Override
        public StaticBuffer getColumn() {
            return getColumnAs(StaticBuffer.STATIC_FACTORY);
        }

        @Override
        public <T> T getColumnAs(Factory<T> factory) {
            return super.as(factory,0,currentValuePos);
        }

        @Override
        public StaticBuffer getValue() {
            return getValueAs(StaticBuffer.STATIC_FACTORY);
        }

        @Override
        public <T> T getValueAs(Factory<T> factory) {
            return super.as(factory,currentValuePos,super.length()-currentValuePos);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;
            if (!(o instanceof StaticBuffer)) return false;
            Entry b = (Entry)o;
            if (getValuePosition()!=b.getValuePosition()) return false;
            return compareTo(getValuePosition(),b,getValuePosition())==0;
        }

        @Override
        public int hashCode() {
            return hashCode(getValuePosition());
        }

        @Override
        public int compareTo(StaticBuffer other) {
            int otherLen = (other instanceof Entry)?((Entry) other).getValuePosition():other.length();
            return compareTo(getValuePosition(),other, otherLen);
        }

        @Override
        public String toString() {
            String s = super.toString();
            int pos = getValuePosition()*4;
            return s.substring(0,pos-1) + "->" + s.substring(pos);
        }

        //########### ITERATOR ##########

        @Override
        public boolean hasNext() {
            return (currentIndex+1)<size();
        }

        @Override
        public Entry next() {
            if (!hasNext()) throw new NoSuchElementException();
            currentIndex++;
            int newOffset = currentIndex>0?getLimit(limitAndValuePos[currentIndex-1]):0;
            metadata = EntryMetaData.EMPTY_METADATA;
            if (StaticArrayEntryList.this.hasMetaData()) {
                metadata = new EntryMetaData.Map();
                newOffset = parseMetaData(metadata,newOffset);
            }
            super.reset(newOffset,getLimit(limitAndValuePos[currentIndex]));
            currentValuePos = getValuePos(limitAndValuePos[currentIndex]);
            return this;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


    //############# CONSTRUCTORS #######################

    public static final EntryList of(Entry... entries) {
        return of(Arrays.asList(entries));
    }


    public static final EntryList of(Iterable<Entry> entries) {
        Preconditions.checkNotNull(entries);
        int num=0;
        int datalen=0;
        EntryMetaData[] metadataschema = null;
        for (Entry entry : entries) {
            num++;
            datalen+=entry.length();
            if (metadataschema==null) metadataschema=StaticArrayEntry.ENTRY_GETTER.getMetaSchema(entry);
            datalen+=getMetaDataSize(metadataschema,entry,StaticArrayEntry.ENTRY_GETTER);
        }
        if (num==0) return EMPTY_LIST;
        byte[] data = new byte[datalen];
        long[] limitAndValuePos = new long[num];
        int pos=0;
        CopyFactory cpf = new CopyFactory(data);
        for (Entry entry : entries) {
            cpf.addMetaData(metadataschema,entry);
            entry.as(cpf);
            limitAndValuePos[pos]= getOffsetandValue(cpf.dataOffset,entry.getValuePosition());
            pos++;
        }
        assert cpf.dataOffset==data.length;
        assert pos==limitAndValuePos.length;
        return new StaticArrayEntryList(data,limitAndValuePos,metadataschema);
    }

    private static class CopyFactory implements StaticBuffer.Factory<Boolean> {

        private final byte[] data;
        private int dataOffset=0;

        private CopyFactory(final byte[] data) {
            this.data=data;
        }

        public void addMetaData(EntryMetaData[] schema, Entry entry) {
            dataOffset=writeMetaData(data,dataOffset,schema,entry,StaticArrayEntry.ENTRY_GETTER);
        }

        @Override
        public Boolean get(byte[] array, int offset, int limit) {
            int len = limit-offset;
            assert len>=0;
            System.arraycopy(array,offset,data,dataOffset,len);
            dataOffset+=len;
            return Boolean.TRUE;
        }
    }

    public static final<E>  EntryList ofBytes(Iterable<E> elements, StaticArrayEntry.GetColVal<E,byte[]> getter) {
        return of(elements, getter, StaticArrayEntry.ByteArrayHandler.INSTANCE);
    }

    public static final<E>  EntryList ofByteBuffer(Iterable<E> elements, StaticArrayEntry.GetColVal<E,ByteBuffer> getter) {
        return of(elements, getter, StaticArrayEntry.ByteBufferHandler.INSTANCE);
    }

    public static final<E>  EntryList ofStaticBuffer(Iterable<E> elements, StaticArrayEntry.GetColVal<E,StaticBuffer> getter) {
        return of(elements, getter, StaticArrayEntry.StaticBufferHandler.INSTANCE);
    }

    public static final<E>  EntryList ofByteBuffer(Iterator<E> elements, StaticArrayEntry.GetColVal<E,ByteBuffer> getter) {
        return of(elements, getter, StaticArrayEntry.ByteBufferHandler.INSTANCE);
    }

    public static final<E>  EntryList ofStaticBuffer(Iterator<E> elements, StaticArrayEntry.GetColVal<E,StaticBuffer> getter) {
        return of(elements, getter, StaticArrayEntry.StaticBufferHandler.INSTANCE);
    }


    private static final<E,D>  EntryList of(Iterable<E> elements, StaticArrayEntry.GetColVal<E,D> getter, StaticArrayEntry.DataHandler<D> datahandler) {
        Preconditions.checkArgument(elements!=null && getter!=null && datahandler!=null);
        int num=0;
        int datalen=0;
        EntryMetaData[] metadataschema = null;
        for (E element : elements) {
            num++;
            datalen+=datahandler.getSize(getter.getColumn(element));
            datalen+=datahandler.getSize(getter.getValue(element));
            if (metadataschema==null) metadataschema=getter.getMetaSchema(element);
            datalen+=getMetaDataSize(metadataschema,element,getter);
        }
        if (num==0) return EMPTY_LIST;
        byte[] data = new byte[datalen];
        long[] limitAndValuePos = new long[num];
        int pos=0;
        int offset=0;
        for (E element : elements) {
            if (element==null) throw new IllegalArgumentException("Unexpected null element in result set");

            offset=writeMetaData(data,offset,metadataschema,element,getter);

            D col = getter.getColumn(element);
            datahandler.copy(col,data,offset);
            int valuePos = datahandler.getSize(col);
            offset+=valuePos;

            D val = getter.getValue(element);
            datahandler.copy(val,data,offset);
            offset+=datahandler.getSize(val);

            limitAndValuePos[pos]= getOffsetandValue(offset,valuePos);
            pos++;
        }
        assert offset==data.length;
        assert pos==limitAndValuePos.length;
        return new StaticArrayEntryList(data,limitAndValuePos,metadataschema);
    }

    private static final<E,D>  EntryList of(Iterator<E> elements, StaticArrayEntry.GetColVal<E,D> getter, StaticArrayEntry.DataHandler<D> datahandler) {
        Preconditions.checkArgument(elements!=null && getter!=null && datahandler!=null);
        if (!elements.hasNext()) return EMPTY_LIST;
        long[] limitAndValuePos = new long[10];
        byte[] data = new byte[limitAndValuePos.length*15];
        EntryMetaData[] metadataschema = null;
        int pos=0;
        int offset=0;
        while (elements.hasNext()) {
            E element = elements.next();
            if (element==null) throw new IllegalArgumentException("Unexpected null element in result set");
            if (metadataschema==null) metadataschema=getter.getMetaSchema(element);

            D col = getter.getColumn(element);
            D val = getter.getValue(element);
            int colSize = datahandler.getSize(col);
            assert colSize>0;
            int valsize = datahandler.getSize(val);
            int metasize = getMetaDataSize(metadataschema,element,getter);

            data = ensureSpace(data,offset,colSize+valsize+metasize);
            offset=writeMetaData(data,offset,metadataschema,element,getter);

            datahandler.copy(col,data,offset);
            offset+=colSize;

            datahandler.copy(val,data,offset);
            offset+=valsize;

            limitAndValuePos = ensureSpace(limitAndValuePos,pos,1);
            limitAndValuePos[pos]= getOffsetandValue(offset,colSize); //valuePosition = colSize
            pos++;
        }
        assert offset<=data.length;
        if (data.length>offset*3/2) {
            //Resize to preserve memory
            byte[] newdata = new byte[offset];
            System.arraycopy(data,0,newdata,0,offset);
            data=newdata;
        }
        if (pos<limitAndValuePos.length) {
            //Resize so that the the array fits exactly
            long[] newPos = new long[pos];
            System.arraycopy(limitAndValuePos,0,newPos,0,pos);
            limitAndValuePos=newPos;
        }
        assert offset<=data.length;
        assert pos==limitAndValuePos.length;
        return new StaticArrayEntryList(data,limitAndValuePos,metadataschema);
    }

    private static byte[] ensureSpace(byte[] data, int offset, int length) {
        if (offset+length<=data.length) return data;
        byte[] newdata = new byte[Math.max(data.length*2,offset+length)];
        System.arraycopy(data,0,newdata,0,offset);
        return newdata;
    }

    //Copy-pasted from above replacing byte->long
    private static long[] ensureSpace(long[] data, int offset, int length) {
        if (offset+length<=data.length) return data;
        long[] newdata = new long[Math.max(data.length*2,offset+length)];
        System.arraycopy(data,0,newdata,0,offset);
        return newdata;
    }

    /* #########################################
            Meta Data Management
     ########################################### */

    /**
     * The integer metaDataSchema is considered as an array of 4bit blocks. If a block equals 0, that means that there is no further
     * meta-data attached. Otherwise, the value translates to the specific meta data as {@code EntryMetaData.values()[value-1]}.
     * </p>
     * This means that
     * 1) metaDataSchema=0 => there is no meta data attached to these entries and
     * 2) We can accommodate at most 15 enum instances in EntryMetaData and at most 8 can be attached to entries.
     */
    private static EntryMetaData[] parseMetaDataSchema(int metaDataSchema) {
        assert EntryMetaData.values().length<15;
        if (metaDataSchema==0) return StaticArrayEntry.EMPTY_SCHEMA;
        int size = 8-Integer.numberOfLeadingZeros(metaDataSchema)/4;
        assert size>0;
        EntryMetaData[] meta = new EntryMetaData[size];
        for (int i=0;i<size;i++) {
            int index = metaDataSchema & 0x0F;
            assert index>0;
            EntryMetaData md = EntryMetaData.values()[index-1];
            assert md!=null;
            meta[i]=md;
        }
        return meta;
    }

    private static int compressMetaDataSchema(EntryMetaData[] metadata) {
        assert EntryMetaData.values().length<15;
        Preconditions.checkArgument(metadata.length<8);
        if (metadata.length==0) return 0;
        int metadataSchema = 0;
        for (int i= metadata.length-1;i>=0;i--) {
            metadataSchema = metadataSchema<<4;
            metadataSchema = metadataSchema | (metadata[i].ordinal()+1);
        }
        assert metadataSchema!=0;
        return metadataSchema;
    }

    private int parseMetaData(Map<EntryMetaData,Object> metadata, int baseOffset) {
        assert hasMetaData();
        for (EntryMetaData meta : metaDataSchema) {
            MetaDataSerializer s = getSerializer(meta);
            Object d = s.read(data,baseOffset);
            baseOffset+=s.getByteLength(d);
            metadata.put(meta,d);
        }
        return baseOffset;
    }

    private static<D,K> int getMetaDataSize(EntryMetaData[] schema, D entry, StaticArrayEntry.GetColVal<D,K> metaGetter) {
        int datasize = 0;
        if (schema.length>0) {
            assert schema.length==metaGetter.getMetaSchema(entry).length;
            for (EntryMetaData meta : schema) {
                Object data = metaGetter.getMetaData(entry,meta);
                assert data!=null;
                datasize+=getSerializer(meta).getByteLength(data);
            }
        }
        return datasize;
    }

    private static<D,K> int writeMetaData(byte[] data, int startPos, EntryMetaData[] schema, D entry, StaticArrayEntry.GetColVal<D,K> metaGetter) {
        if (schema.length==0) return startPos;
        for (EntryMetaData meta : schema) {
            Object d = metaGetter.getMetaData(entry,meta);
            assert d!=null;
            MetaDataSerializer s = getSerializer(meta);
            s.write(data,startPos,d);
            startPos+=s.getByteLength(d);
        }
        return startPos;
    }

    public static MetaDataSerializer getSerializer(EntryMetaData meta) {
        switch (meta) {
            case TTL:
                return IntSerializer.INSTANCE;
            case TIMESTAMP:
                return LongSerializer.INSTANCE;
            case VISIBILITY:
                return ASCIIStringSerializer.INSTANCE;
            default: throw new AssertionError("Unexpected meta data: " + meta);
        }
    }

    public static interface MetaDataSerializer<V> {

        int getByteLength(V value);

        public void write(byte[] data, int startPos, V value);

        public V read(byte[] data, int startPos);

    }

    private static enum IntSerializer implements MetaDataSerializer<Integer> {

        INSTANCE;

        @Override
        public int getByteLength(Integer value) {
            return 4;
        }

        @Override
        public void write(byte[] data, int startPos, Integer value) {
            assert data.length>=startPos+getByteLength(value);
            assert value!=null;
            StaticArrayBuffer.putInt(data, startPos, value);
        }

        @Override
        public Integer read(byte[] data, int startPos) {
            assert data.length>=startPos+4;
            return StaticArrayBuffer.getInt(data, startPos);
        }
    }
      
    private static enum LongSerializer implements MetaDataSerializer<Long> {

        INSTANCE;

        @Override
        public int getByteLength(Long value) {
            return 8;
        }

        @Override
        public void write(byte[] data, int startPos, Long value) {
            assert data.length>=startPos+getByteLength(value);
            assert value!=null;
            StaticArrayBuffer.putLong(data,startPos,value);
        }

        @Override
        public Long read(byte[] data, int startPos) {
            assert data.length>=startPos+8;
            return StaticArrayBuffer.getLong(data,startPos);
        }
    }

    private static enum ASCIIStringSerializer implements MetaDataSerializer<String> {

        INSTANCE;


        @Override
        public int getByteLength(String value) {
            return StringEncoding.getAsciiByteLength(value);
        }

        @Override
        public void write(byte[] data, int startPos, String value) {
            assert data.length>=startPos+getByteLength(value);
            StringEncoding.writeAsciiString(data,startPos,value);
        }

        @Override
        public String read(byte[] data, int startPos) {
            return StringEncoding.readAsciiString(data,startPos);
        }
    }

}
