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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.collections.comparators.ComparableComparator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PointInterval<T> implements Interval<T> {

    private final Set<T> points;

    private PointInterval(Set<T> points) {
        this.points = points;
    }

    public PointInterval(T point) {
        points= new HashSet<>(1);
        points.add(point);
    }

    public PointInterval(T... points) {
        this(Arrays.asList(points));
    }

    public PointInterval(Iterable<T> points) {
        this.points= new HashSet<>(4);
        Iterables.addAll(this.points,points);
    }

    @Override
    public Collection<T> getPoints() {
        return points;
    }

    public void setPoint(T point) {
        points.clear();
        points.add(point);
    }

    public void addPoint(T point) {
        points.add(point);
    }

    @Override
    public T getStart() {
        Preconditions.checkArgument(!isEmpty(),"There are no points in this interval");
        return (T)Collections.min(points,ComparableComparator.getInstance());
    }

    @Override
    public T getEnd() {
        Preconditions.checkArgument(!isEmpty(), "There are no points in this interval");
        return (T)Collections.max(points,ComparableComparator.getInstance());
    }

    @Override
    public boolean startInclusive() {
        return true;
    }

    @Override
    public boolean endInclusive() {
        return true;
    }

    @Override
    public boolean isPoints() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return points.isEmpty();
    }

    @Override
    public Interval<T> intersect(Interval<T> other) {
        Preconditions.checkArgument(other!=null);
        if (other instanceof PointInterval) {
            Sets.newHashSet(points);
            points.retainAll(((PointInterval)other).points);
            return new PointInterval<>(points);
        } else if (other instanceof RangeInterval) {
            final RangeInterval<T> rint = (RangeInterval)other;
            return new PointInterval<>(Sets.newHashSet(Iterables.filter(points, rint::containsPoint)));
        } else throw new AssertionError("Unexpected interval: " + other);
    }

    @Override
    public int hashCode() {
        return points.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        PointInterval oth = (PointInterval)other;
        return points.equals(oth.points);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("[");
        int i = 0;
        for (T point : points) {
            if (i>0) s.append(",");
            s.append(point);
            i++;
        }
        s.append("]");
        return s.toString();
    }
}
