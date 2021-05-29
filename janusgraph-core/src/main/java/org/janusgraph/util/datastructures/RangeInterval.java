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

package org.janusgraph.util.datastructures;

import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RangeInterval<T> implements Interval<T> {

    private boolean startInclusive=true;
    private boolean endInclusive=true;
    private T start = null;
    private T end = null;

    public RangeInterval() {

    }

    public RangeInterval(T start, T end) {
        setStart(start,true);
        setEnd(end,true);
    }

    public RangeInterval<T> setStart(T start, boolean inclusive) {
        Preconditions.checkArgument(start instanceof Comparable);
        this.start=start;
        setStartInclusive(inclusive);
        return this;
    }

    public RangeInterval<T> setEnd(T end, boolean inclusive) {
        Preconditions.checkArgument(end instanceof Comparable);
        this.end=end;
        setEndInclusive(inclusive);
        return this;
    }

    public RangeInterval<T> setStartInclusive(boolean inclusive) {
        Preconditions.checkArgument(start==null || start instanceof  Comparable);
        this.startInclusive=inclusive;
        return this;
    }

    public RangeInterval<T> setEndInclusive(boolean inclusive) {
        Preconditions.checkArgument(end==null || end instanceof  Comparable);
        this.endInclusive=inclusive;
        return this;
    }

    @Override
    public T getStart() {
        return start;
    }

    @Override
    public T getEnd() {
        return end;
    }

    @Override
    public boolean startInclusive() {
        return startInclusive;
    }

    @Override
    public boolean endInclusive() {
        return endInclusive;
    }

    @Override
    public boolean isPoints() {
        return start!=null && end!=null && start.equals(end) && startInclusive && endInclusive;
    }

    @Override
    public Collection<T> getPoints() {
        final Set<T> set = new HashSet<>(2);
        if (start!=null) set.add(start);
        if (end!=null) set.add(end);
        return set;
    }

    @Override
    public boolean isEmpty() {
        if (start==null || end==null) return false;
        if (isPoints()) return false;
        int cmp = ((Comparable)start).compareTo(end);
        return cmp>0 || (cmp==0 && (!startInclusive || !endInclusive));
    }


    @Override
    public Interval<T> intersect(Interval<T> other) {
        Preconditions.checkArgument(other!=null);
        if (other instanceof PointInterval) {
            return other.intersect(this);
        } else if (other instanceof RangeInterval) {
            final RangeInterval<T> rint = (RangeInterval)other;
            final Map.Entry<T,Boolean> newStart = comparePoints(start,startInclusive,rint.start,rint.startInclusive,true);
            final Map.Entry<T,Boolean> newEnd = comparePoints(end,endInclusive,rint.end,rint.endInclusive,false);
            final RangeInterval<T> result = new RangeInterval<>();
            if (newStart.getKey() != null) {
                result.setStart(newStart.getKey(), newStart.getValue());
            }
            if (newEnd.getKey() != null) {
                result.setEnd(newEnd.getKey(), newEnd.getValue());
            }
            return result;
        } else throw new AssertionError("Unexpected interval: " + other);
    }

    private Map.Entry<T,Boolean> comparePoints(T one, boolean oneIncl, T two, boolean twoIncl, boolean chooseBigger) {
        if (one==null) return new AbstractMap.SimpleImmutableEntry<>(two, twoIncl);
        if (two==null) return new AbstractMap.SimpleImmutableEntry<>(one, oneIncl);
        int c = ((Comparable)one).compareTo(two);
        if (c==0) {
            return new AbstractMap.SimpleImmutableEntry<>(one, oneIncl & twoIncl);
        } else if ((c>0 && chooseBigger) || (c<0  && !chooseBigger)) {
            return new AbstractMap.SimpleImmutableEntry<>(one, oneIncl);
        } else {
            return new AbstractMap.SimpleImmutableEntry<>(two, twoIncl);
        }
    }

    public boolean containsPoint(T other) {
        Preconditions.checkNotNull(other);
        if (isPoints()) return start.equals(other);
        else {
            if (start!=null) {
                int cmp = ((Comparable)start).compareTo(other);
                if (cmp>0 || (cmp==0 && !startInclusive)) return false;
            }
            if (end!=null) {
                int cmp = ((Comparable)end).compareTo(other);
                return cmp >= 0 && (cmp != 0 || endInclusive);
            }
            return true;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, startInclusive, endInclusive);
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        RangeInterval oth = (RangeInterval)other;
        return Objects.equals(start, oth.start) && Objects.equals(end, oth.end) && end.equals(oth.end) && endInclusive==oth.endInclusive && startInclusive==oth.startInclusive;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (startInclusive) b.append("[");
        else b.append("(");
        b.append(start).append(",").append(end);
        if (endInclusive) b.append("]");
        else b.append(")");
        return b.toString();
    }
}
