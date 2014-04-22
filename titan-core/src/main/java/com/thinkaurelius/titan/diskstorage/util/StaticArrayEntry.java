package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.nio.ByteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntry extends BaseStaticArrayEntry {

    public StaticArrayEntry(byte[] array, int offset, int limit, int valuePosition) {
        super(array, offset, limit, valuePosition);
    }

    public StaticArrayEntry(byte[] array, int limit, int valuePosition) {
        super(array, limit, valuePosition);
    }

    public StaticArrayEntry(byte[] array, int valuePosition) {
        super(array, valuePosition);
    }

    public StaticArrayEntry(StaticBuffer buffer, int valuePosition, Integer ttl) {
        super(buffer, valuePosition, ttl);
    }

    /**
     * ############# IDENTICAL CODE ###############
     */

    private volatile transient RelationCache cache;

    @Override
    public RelationCache getCache() {
        return cache;
    }

    @Override
    public void setCache(RelationCache cache) {
        Preconditions.checkNotNull(cache);
        this.cache = cache;
    }

    //########### CONSTRUCTORS AND UTILITIES ###########

    public static Entry of(StaticBuffer buffer) {
        return new StaticArrayEntry(buffer,buffer.length(),null);
    }

    public static final<E> Entry ofBytes(E element, StaticArrayEntry.GetColVal<E,byte[]> getter) {
        return of(element, getter, ByteArrayHandler.INSTANCE);
    }

    public static final<E>  Entry ofByteBuffer(E element, StaticArrayEntry.GetColVal<E,ByteBuffer> getter) {
        return of(element, getter, ByteBufferHandler.INSTANCE);
    }

    public static final<E>  Entry ofStaticBuffer(E element, StaticArrayEntry.GetColVal<E,StaticBuffer> getter) {
        return of(element, getter, StaticBufferHandler.INSTANCE);
    }

    public static final<E>  Entry of(StaticBuffer column, StaticBuffer value) {
        return of(column, value, StaticBufferHandler.INSTANCE);
    }

    private static final<E,D>  Entry of(E element, StaticArrayEntry.GetColVal<E,D> getter, StaticArrayEntry.DataHandler<D> datahandler) {
        return of(getter.getColumn(element),getter.getValue(element),datahandler);
    }

    private static final<E,D>  Entry of(D column, D value, StaticArrayEntry.DataHandler<D> datahandler) {
        int valuePos = datahandler.getSize(column);
        byte[] data = new byte[valuePos+datahandler.getSize(value)];
        datahandler.copy(column,data,0);
        datahandler.copy(value,data,valuePos);
        return new StaticArrayEntry(data,valuePos);
    }

    public static interface GetColVal<E,D> {

        public D getColumn(E element);

        public D getValue(E element);

    }

    public static GetColVal<Entry,StaticBuffer> ENTRY_GETTER = new GetColVal<Entry, StaticBuffer>() {
        @Override
        public StaticBuffer getColumn(Entry entry) {
            return entry.getColumn();
        }

        @Override
        public StaticBuffer getValue(Entry entry) {
            return entry.getValue();
        }
    };

    public static interface DataHandler<D> {

        public int getSize(D data);

        public void copy(D data, byte[] dest, int destOffset);

    }


    static enum ByteArrayHandler implements DataHandler<byte[]> {

        INSTANCE;

        @Override
        public int getSize(byte[] data) {
            return data.length;
        }

        @Override
        public void copy(byte[] data, byte[] dest, int destOffset) {
            System.arraycopy(data,0,dest,destOffset,data.length);
        }
    }

    static enum ByteBufferHandler implements DataHandler<ByteBuffer> {

        INSTANCE;

        @Override
        public int getSize(ByteBuffer data) {
            return data.remaining();
        }

        @Override
        public void copy(ByteBuffer data, byte[] dest, int destOffset) {
            if (data.hasArray()) {
                System.arraycopy(data.array(),data.arrayOffset()+data.position(),dest,destOffset,data.remaining());
            } else {
                data.mark();
                data.get(dest,destOffset,data.remaining());
                data.reset();
            }
        }
    }

    static enum StaticBufferHandler implements DataHandler<StaticBuffer> {

        INSTANCE;

        @Override
        public int getSize(StaticBuffer data) {
            return data.length();
        }

        @Override
        public void copy(StaticBuffer data, byte[] dest, int destOffset) {
            if (data instanceof StaticArrayBuffer) {
                StaticArrayBuffer buffer = (StaticArrayBuffer) data;
                buffer.copyTo(dest,destOffset);
            } else throw new IllegalArgumentException("Expected StaticArrayBuffer but got: " + data.getClass());
        }
    }

}

class BaseStaticArrayEntry extends StaticArrayBuffer implements Entry {

    private final int valuePosition;
    private final Integer ttl;

    public BaseStaticArrayEntry(byte[] array, int offset, int limit, int valuePosition) {
        super(array,offset,limit);
        Preconditions.checkArgument(valuePosition>0);
        Preconditions.checkArgument(valuePosition<=length());
        this.valuePosition=valuePosition;
        ttl = null;
    }

    public BaseStaticArrayEntry(byte[] array, int limit, int valuePosition) {
        this(array, 0, limit, valuePosition);
    }

    public BaseStaticArrayEntry(byte[] array, int valuePosition) {
        this(array, 0, array.length, valuePosition);
    }

    public BaseStaticArrayEntry(StaticBuffer buffer, int valuePosition, Integer ttl) {
        super(buffer);
        Preconditions.checkArgument(valuePosition>0);
        Preconditions.checkArgument(valuePosition<=length());
        this.valuePosition=valuePosition;
        this.ttl = ttl;
    }

    @Override
    public Integer getTtl() {
        return ttl;
    }

    @Override
    public int getValuePosition() {
        return valuePosition;
    }

    @Override
    public boolean hasValue() {
        return valuePosition<length();
    }

    @Override
    public StaticBuffer getColumn() {
        return getColumnAs(StaticBuffer.STATIC_FACTORY);
    }

    @Override
    public <T> T getColumnAs(Factory<T> factory) {
        return super.as(factory,0,valuePosition);
    }

    @Override
    public StaticBuffer getValue() {
        return getValueAs(StaticBuffer.STATIC_FACTORY);
    }

    @Override
    public <T> T getValueAs(Factory<T> factory) {
        return super.as(factory,valuePosition,super.length()-valuePosition);
    }

    //Override from StaticArrayBuffer to restrict to column

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

    //########## CACHE ############

    @Override
    public RelationCache getCache() {
        return null;
    }

    @Override
    public void setCache(RelationCache cache) {
        throw new UnsupportedOperationException();
    }

}
