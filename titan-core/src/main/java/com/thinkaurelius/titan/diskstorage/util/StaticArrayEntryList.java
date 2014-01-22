package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntryList extends AbstractList<Entry> implements EntryList {

    private final byte[] data;
    private final long[] limitAndValuePos;

    private final RelationCache[] caches;

    private StaticArrayEntryList(final byte[] data, final long[] limitAndValuePos) {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(limitAndValuePos!=null && limitAndValuePos.length>0);
        this.data=data;
        this.limitAndValuePos=limitAndValuePos;
        this.caches = new RelationCache[limitAndValuePos.length];
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

    @Override
    public Entry get(int index) {
        Preconditions.checkPositionIndex(index,limitAndValuePos.length);
        return new StaticEntry(index);
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

        public StaticEntry(final int index) {
            super(data, index>0?getLimit(limitAndValuePos[index-1]):0,
                    getLimit(limitAndValuePos[index]), getValuePos(limitAndValuePos[index]));
            this.index=index;
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
        for (Entry entry : entries) {
            num++;
            datalen+=entry.length();
        }
        if (num==0) return EMPTY_LIST;
        byte[] data = new byte[datalen];
        long[] limitAndValuePos = new long[num];
        int pos=0;
        CopyFactory cpf = new CopyFactory(data);
        for (Entry entry : entries) {
            entry.as(cpf);
            limitAndValuePos[pos]= getOffsetandValue(cpf.dataOffset,entry.getValuePosition());
            pos++;
        }
        assert cpf.dataOffset==data.length;
        assert pos==limitAndValuePos.length;
        return new StaticArrayEntryList(data,limitAndValuePos);
    }

    private static class CopyFactory implements StaticBuffer.Factory<Boolean> {

        private final byte[] data;
        private int dataOffset=0;

        private CopyFactory(final byte[] data) {
            this.data=data;
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
        for (E element : elements) {
            num++;
            datalen+=datahandler.getSize(getter.getColumn(element));
            datalen+=datahandler.getSize(getter.getValue(element));
        }
        if (num==0) return EMPTY_LIST;
        byte[] data = new byte[datalen];
        long[] limitAndValuePos = new long[num];
        int pos=0;
        int offset=0;
        for (E element : elements) {
            if (element==null) throw new IllegalArgumentException("Unexpected null element in result set");

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
        return new StaticArrayEntryList(data,limitAndValuePos);
    }

    private static final<E,D>  EntryList of(Iterator<E> elements, StaticArrayEntry.GetColVal<E,D> getter, StaticArrayEntry.DataHandler<D> datahandler) {
        Preconditions.checkArgument(elements!=null && getter!=null && datahandler!=null);
        if (!elements.hasNext()) return EMPTY_LIST;
        long[] limitAndValuePos = new long[10];
        byte[] data = new byte[limitAndValuePos.length*15];
        int pos=0;
        int offset=0;
        while (elements.hasNext()) {
            E element = elements.next();
            if (element==null) throw new IllegalArgumentException("Unexpected null element in result set");

            D col = getter.getColumn(element);
            D val = getter.getValue(element);
            int colSize = datahandler.getSize(col);
            assert colSize>0;
            int valsize = datahandler.getSize(val);

            data = ensureSpace(data,offset,colSize+valsize);
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
        return new StaticArrayEntryList(data,limitAndValuePos);
    }

    private static byte[] ensureSpace(byte[] data, int offset, int length) {
        if (offset+length<=data.length) return data;
        byte[] newdata = new byte[data.length*2];
        System.arraycopy(data,0,newdata,0,offset);
        return newdata;
    }

    //Copy-pasted from above replacing byte->long
    private static long[] ensureSpace(long[] data, int offset, int length) {
        if (offset+length<=data.length) return data;
        long[] newdata = new long[data.length*2];
        System.arraycopy(data,0,newdata,0,offset);
        return newdata;
    }

}
