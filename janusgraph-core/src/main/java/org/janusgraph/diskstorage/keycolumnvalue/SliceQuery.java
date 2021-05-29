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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.graphdb.query.BackendQuery;
import org.janusgraph.graphdb.query.BaseQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Queries for a slice of data identified by a start point (inclusive) and end point (exclusive).
 * Returns all {@link StaticBuffer}s that lie in this range up to the given limit.
 * <p>
 * If a SliceQuery is marked <i>static</i> it is expected that the result set does not change.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SliceQuery extends BaseQuery implements BackendQuery<SliceQuery> {

    private final StaticBuffer sliceStart;
    private final StaticBuffer sliceEnd;
    private String type;

    public SliceQuery(final StaticBuffer sliceStart, final StaticBuffer sliceEnd, final String type) {
        this(sliceStart, sliceEnd);
        this.type = type;
    }

    public SliceQuery(final StaticBuffer sliceStart, final StaticBuffer sliceEnd) {
        assert sliceStart != null && sliceEnd != null;

        this.sliceStart = sliceStart;
        this.sliceEnd = sliceEnd;
    }

    public SliceQuery(final SliceQuery query) {
        this(query.getSliceStart(), query.getSliceEnd());
        setLimit(query.getLimit());
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
        return Objects.hash(sliceStart, sliceEnd, getLimit());
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
                && getLimit() == oth.getLimit();
    }

    public boolean subsumes(SliceQuery oth) {
        Preconditions.checkNotNull(oth);
        if (this == oth) return true;
        if (oth.getLimit() > getLimit()) return false;
        else if (!hasLimit()) //the interval must be subsumed
            return sliceStart.compareTo(oth.sliceStart) <= 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
        else //this the result might be cutoff due to limit, the start must be the same
            return sliceStart.compareTo(oth.sliceStart) == 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
    }

    //TODO: make this more efficient by using reuseIterator() on otherResult
    public EntryList getSubset(final SliceQuery otherQuery, final EntryList otherResult) {
        assert otherQuery.subsumes(this);
        int pos = Collections.binarySearch(otherResult, sliceStart);
        if (pos < 0) pos = -pos - 1;

        final List<Entry> result = new ArrayList<>();
        for (; pos < otherResult.size() && result.size() < getLimit(); pos++) {
            Entry e = otherResult.get(pos);
            if (e.getColumnAs(StaticBuffer.STATIC_FACTORY).compareTo(sliceEnd) < 0) result.add(e);
            else break;
        }
        return EntryArrayList.of(result);
    }

    public boolean contains(StaticBuffer buffer) {
        return sliceStart.compareTo(buffer)<=0 && sliceEnd.compareTo(buffer)>0;
    }

    public static StaticBuffer pointRange(StaticBuffer point) {
        return BufferUtil.nextBiggerBuffer(point);
    }

    @Override
    public SliceQuery setLimit(int limit) {
        Preconditions.checkArgument(!hasLimit());
        super.setLimit(limit);
        return this;
    }

    @Override
    public SliceQuery updateLimit(int newLimit) {
        return new SliceQuery(sliceStart, sliceEnd).setLimit(newLimit);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (type != null) {
            sb.append(type).append(":");
        }
        sb.append("SliceQuery[").append(getSliceStart()).append(",").append(getSliceEnd()).append(")");
        if (hasLimit()) sb.append("@").append(getLimit());
        return sb.toString();
    }

}
