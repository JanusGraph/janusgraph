package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Extends a {@link SliceQuery} to express a range for columns and a range for
 * keys. Selects each key on the interval
 * {@code [keyStart inclusive, keyEnd exclusive)} for which there exists at
 * least one column between {@code [sliceStart inclusive, sliceEnd exclusive)}.
 * <p>
 * The limit of a KeyRangeQuery applies to the maximum number of columns
 * returned per key which fall into the specified slice range and NOT to the
 * maximum number of keys returned.
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyRangeQuery extends SliceQuery {

    private final StaticBuffer keyStart;
    private final StaticBuffer keyEnd;


    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd, boolean isStatic) {
        super(sliceStart, sliceEnd, isStatic);
        Preconditions.checkNotNull(keyStart);
        Preconditions.checkNotNull(keyEnd);
        this.keyStart=keyStart;
        this.keyEnd = keyEnd;
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, SliceQuery query) {
        super(query);
        Preconditions.checkNotNull(keyStart);
        Preconditions.checkNotNull(keyEnd);
        this.keyStart=keyStart;
        this.keyEnd = keyEnd;
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        this(keyStart,keyEnd,sliceStart,sliceEnd,DEFAULT_STATIC);
    }


    public StaticBuffer getKeyStart() {
        return keyStart;
    }

    public StaticBuffer getKeyEnd() {
        return keyEnd;
    }

    @Override
    public KeyRangeQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public KeyRangeQuery updateLimit(int newLimit) {
        return new KeyRangeQuery(keyStart,keyEnd,this).setLimit(newLimit);
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(keyStart).append(keyEnd).appendSuper(super.hashCode()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        KeyRangeQuery oth = (KeyRangeQuery)other;
        return keyStart.equals(oth.keyStart) && keyEnd.equals(oth.keyEnd) && super.equals(oth);
    }

    public boolean subsumes(KeyRangeQuery oth) {
        return super.subsumes(oth) && keyStart.compareTo(oth.keyStart)<=0 && keyEnd.compareTo(oth.keyEnd)>=0;
    }

    @Override
    public String toString() {
        return String.format("KeyRangeQuery(start: %s, end: %s, columns:[start: %s, end: %s], limit=%d)",
                             keyStart,
                             keyEnd,
                             getSliceStart(),
                             getSliceEnd(),
                             getLimit());
    }
}
