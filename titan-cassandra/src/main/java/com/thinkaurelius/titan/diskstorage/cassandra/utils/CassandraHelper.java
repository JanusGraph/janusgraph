package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import javax.annotation.Nullable;

public class CassandraHelper {

    public static List<ByteBuffer> convert(List<StaticBuffer> keys) {
        List<ByteBuffer> requestKeys = new ArrayList<ByteBuffer>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            requestKeys.add(keys.get(i).asByteBuffer());
        }
        return requestKeys;
    }

    /**
     * Constructs an {@link EntryList} from the Iterable of entries while excluding the end slice
     * (since the method contract states that the end slice is exclusive, yet Cassandra treats it as
     * inclusive) and respecting the limit.
     *
     * @param entries
     * @param getter
     * @param lastColumn TODO: make this StaticBuffer so we can avoid the conversion and provide equals method
     * @param limit
     * @param <E>
     * @return
     */
    public static<E> EntryList makeEntryList(final Iterable<E> entries,
                                             final StaticArrayEntry.GetColVal<E,ByteBuffer> getter,
                                             final StaticBuffer lastColumn, final int limit) {
        return StaticArrayEntryList.ofByteBuffer(new Iterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return Iterators.filter(entries.iterator(),new FilterResultColumns<E>(lastColumn,limit,getter));
            }
        },getter);
    }

    private static class FilterResultColumns<E> implements Predicate<E> {

        private int count = 0;

        private final int limit;
        private final StaticBuffer lastColumn;
        private final StaticArrayEntry.GetColVal<E,ByteBuffer> getter;

        private FilterResultColumns(StaticBuffer lastColumn, int limit, StaticArrayEntry.GetColVal<E, ByteBuffer> getter) {
            this.limit = limit;
            this.lastColumn = lastColumn;
            this.getter = getter;
        }

        @Override
        public boolean apply(@Nullable E e) {
            assert e!=null;
            if (count>=limit || BufferUtil.equals(lastColumn, getter.getColumn(e))) return false;
            count++;
            return true;
        }

    }

    public static<E> Iterator<Entry> makeEntryIterator(final Iterable<E> entries,
                                             final StaticArrayEntry.GetColVal<E,ByteBuffer> getter,
                                             final StaticBuffer lastColumn, final int limit) {
        return Iterators.transform(Iterators.filter(entries.iterator(),
                new FilterResultColumns<E>(lastColumn, limit, getter)), new Function<E, Entry>() {
            @Nullable
            @Override
            public Entry apply(@Nullable E e) {
                return StaticArrayEntry.ofByteBuffer(e,getter);
            }
        });
    }


    public static KeyRange transformRange(Range<Token> range) {
        return transformRange(range.left, range.right);
    }

    public static KeyRange transformRange(Token leftKeyExclusive, Token rightKeyInclusive) {
        if (!(leftKeyExclusive instanceof BytesToken))
            throw new UnsupportedOperationException();

        // if left part is BytesToken, right part should be too, otherwise there is no sense in the ring
        assert rightKeyInclusive instanceof BytesToken;

        // l is exclusive, r is inclusive
        BytesToken l = (BytesToken) leftKeyExclusive;
        BytesToken r = (BytesToken) rightKeyInclusive;

        byte[] leftTokenValue = l.getTokenValue();
        byte[] rightTokenValue = r.getTokenValue();

        Preconditions.checkArgument(leftTokenValue.length == rightTokenValue.length, "Tokens have unequal length");
        int tokenLength = leftTokenValue.length;

        byte[][] tokens = new byte[][]{leftTokenValue, rightTokenValue};
        byte[][] plusOne = new byte[2][tokenLength];

        for (int j = 0; j < 2; j++) {
            boolean carry = true;
            for (int i = tokenLength - 1; i >= 0; i--) {
                byte b = tokens[j][i];
                if (carry) {
                    b++;
                    carry = false;
                }
                if (b == 0) carry = true;
                plusOne[j][i] = b;
            }
        }

        StaticBuffer lb = StaticArrayBuffer.of(plusOne[0]);
        StaticBuffer rb = StaticArrayBuffer.of(plusOne[1]);
        Preconditions.checkArgument(lb.length() == tokenLength, lb.length());
        Preconditions.checkArgument(rb.length() == tokenLength, rb.length());

        return new KeyRange(lb, rb);
    }
}
