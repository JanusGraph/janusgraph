package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.query.BackendQuery;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Queries for a slice of data identified by a start point (inclusive) and end point (exclusive).
 * Returns all {@link StaticBuffer}s that lie in this range up to the given limit.
 * <p/>
 * If a SliceQuery is marked <i>static</i> it is expected that the result set does not change.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SliceQuery extends BaseQuery implements BackendQuery<SliceQuery> {

    public static final boolean DEFAULT_STATIC = false;

    private final StaticBuffer sliceStart;
    private final StaticBuffer sliceEnd;

    private final boolean isStatic;

    protected int hashcode;

    public SliceQuery(final StaticBuffer sliceStart, final StaticBuffer sliceEnd, boolean isStatic) {
        assert sliceStart != null && sliceEnd != null;

        this.sliceStart = sliceStart;
        this.sliceEnd = sliceEnd;
        this.isStatic = isStatic;
    }

    public SliceQuery(final StaticBuffer sliceStart, final StaticBuffer sliceEnd) {
        this(sliceStart, sliceEnd, DEFAULT_STATIC);
    }

    public SliceQuery(final SliceQuery query) {
        this(query.getSliceStart(), query.getSliceEnd(), query.isStatic());
        setLimit(query.getLimit());
    }


    /**
     * Whether the result set, if non-empty, is static, i.e. does not
     * change anymore. This allows the query result (if non-empty) to be cached.
     *
     * @return whether query is static
     */
    public boolean isStatic() {
        return isStatic;
    }

    /**
     * The start of the slice is considered to be inclusive
     *
     * @return The StaticBuffer denoting the start of the slice
     */
    public StaticBuffer getSliceStart() {
        return sliceStart;
    }

    /**
     * The end of the slice is considered to be exclusive
     *
     * @return The StaticBuffer denoting the end of the slice
     */
    public StaticBuffer getSliceEnd() {
        return sliceEnd;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(sliceStart).append(sliceEnd).append(isStatic).append(getLimit()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null && !getClass().isInstance(other))
            return false;

        SliceQuery oth = (SliceQuery) other;
        return sliceStart.equals(oth.sliceStart)
                && sliceEnd.equals(oth.sliceEnd)
                && getLimit() == oth.getLimit()
                && isStatic == oth.isStatic;
    }

    //TODO: Need to update when introducing DESC
    public boolean subsumes(SliceQuery oth) {
        Preconditions.checkNotNull(oth);
        if (this == oth) return true;
        if (oth.getLimit() > getLimit()) return false;
        else if (!hasLimit()) //the interval must be subsumed
            return sliceStart.compareTo(oth.sliceStart) <= 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
        else //this the result might be cutoff due to limit, the start must be the same
            return sliceStart.compareTo(oth.sliceStart) == 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
    }

    public List<Entry> getSubset(SliceQuery otherQuery, List<Entry> otherResult) {
        assert otherQuery.subsumes(this);
        List<Entry> result = new ArrayList<Entry>();
        int pos = Collections.binarySearch(result, StaticBufferEntry.of(sliceStart));
        if (pos < 0) pos = -pos - 1;
        for (; pos < otherResult.size() && result.size() < getLimit(); pos++) {
            Entry e = otherResult.get(pos);
            if (e.getColumn().compareTo(sliceEnd) < 0) result.add(e);
            else break;
        }
        return result;
    }

    public static StaticBuffer pointRange(StaticBuffer point) {
        return ByteBufferUtil.nextBiggerBuffer(point);
    }

    @Override
    public SliceQuery setLimit(int limit) {
        Preconditions.checkArgument(!hasLimit());
        super.setLimit(limit);
        return this;
    }

    @Override
    public SliceQuery updateLimit(int newLimit) {
        return new SliceQuery(sliceStart, sliceEnd, isStatic).setLimit(newLimit);
    }

}
