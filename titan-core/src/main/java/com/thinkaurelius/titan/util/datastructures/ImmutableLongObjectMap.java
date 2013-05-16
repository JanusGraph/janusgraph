package com.thinkaurelius.titan.util.datastructures;

import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Immutable map from long key ids to objects.
 * Implemented for memory and time efficiency.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImmutableLongObjectMap implements Iterable<ImmutableLongObjectMap.Entry> {

    private final long[] keys;
    private final Object[] values;

    public ImmutableLongObjectMap(final long[] keysUnsorted, final Object[] valuesUnsorted, final int size) {
        Preconditions.checkNotNull(keysUnsorted);
        Preconditions.checkNotNull(valuesUnsorted);
        Preconditions.checkArgument(size>=0);
        LongIntMap indexmap = new LongIntOpenHashMap(size);
        for(int i=0;i<size;i++) {
            indexmap.put(keysUnsorted[i],i);
        }
        Preconditions.checkArgument(indexmap.size()==size,"Duplicate keys detected");
        this.keys = new long[size];
        System.arraycopy(keysUnsorted,0,keys,0,size);
        Arrays.sort(keys);
        this.values = new Object[keys.length];
        for (int i=0;i<keys.length;i++) {
            values[i]=valuesUnsorted[indexmap.get(keys[i])];
        }
        Preconditions.checkArgument(keys.length==values.length);
    }


    public <O> O get(long key) {
        int pos = Arrays.binarySearch(keys,key);
        if (pos<0) return null;
        else return (O)values[pos];
    }

    public long getKey(int index) {
        Preconditions.checkArgument(index>=0 && index<keys.length);
        return keys[index];
    }

    public Object getValue(int index) {
        Preconditions.checkArgument(index>=0 && index<keys.length);
        return values[index];
    }

    public int size() {
        return keys.length;
    }

    @Override
    public Iterator<Entry> iterator() {
        return new Iterator<Entry>() {

            int index = 0;

            @Override
            public boolean hasNext() {
                return index<keys.length;
            }

            @Override
            public Entry next() {
                Entry next = new Entry(index);
                index++;
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public class Entry {

        private final int index;

        private Entry(int index) {
            Preconditions.checkArgument(index>=0 && index<keys.length);
            this.index=index;
        }

        public long getKey() {
            return keys[index];
        }

        public Object getValue() {
            return values[index];
        }

    }

    public static class Builder {

        private static final int INITIAL = 4;
        private static final int MULTIPLE = 2;

        private long[] keys;
        private Object[] values;
        private int size;

        public Builder() {
            keys = new long[INITIAL];
            values = new Object[INITIAL];
            size = 0;
        }

        public void put(long key, Object value) {
            if (keys.length==size) {
                //Expand
                long[] newkeys = new long[size*MULTIPLE];
                Object[] newvalues = new Object[size*MULTIPLE];
                System.arraycopy(keys,0,newkeys,0,size);
                System.arraycopy(values,0,newvalues,0,size);
                keys = newkeys;
                values= newvalues;
            }
            Preconditions.checkArgument(keys.length>size && keys.length==values.length);
            keys[size]=key;
            values[size]=value;
            size++;
        }

        public int size() {
            return size;
        }

        public ImmutableLongObjectMap build() {
            return new ImmutableLongObjectMap(keys,values,size);
        }


    }
}
