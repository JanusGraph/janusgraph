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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Utility class for interacting with {@link Iterable}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IterablesUtil {

    public static <O> Iterable<O> emptyIterable() {
        return Collections::emptyIterator;
    }

    public static final Predicate NO_FILTER = new NoFilter();

    public static <E> Predicate<E> noFilter() {
        return (Predicate<E>)NO_FILTER;
    }

    private static class NoFilter<E> implements Predicate<E> {

        @Override
        public boolean apply(@Nullable E e) {
            return true;
        }
    }

    public static <O> Iterable<O> limitedIterable(final Iterable<O> iterable, final int limit) {
        return StreamSupport.stream(iterable.spliterator(), false).limit(limit).collect(Collectors.toList());
    }

    public static int size(Iterable i) {
        if (i instanceof Collection) return ((Collection)i).size();
        else return Iterables.size(i);
    }

    public static boolean sizeLargerOrEqualThan(Iterable i, int limit) {
        if (i instanceof Collection) return ((Collection)i).size()>=limit;
        Iterator iterator = i.iterator();
        int count=0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
            if (count>=limit) return true;
        }
        return false;
    }

    public static<E> List<E> mergeSort(Collection<E> a, Collection<E> b, Comparator<E> comp) {
        Iterator<E> iteratorA = a.iterator(), iteratorB = b.iterator();
        E headA = iteratorA.hasNext()?iteratorA.next():null;
        E headB = iteratorB.hasNext()?iteratorB.next():null;
        List<E> result = new ArrayList<>(a.size()+b.size());
        while (headA!=null || headB!=null) {
            E next;
            if (headA==null) {
                next=headB;
                headB = null;
            } else if (headB==null) {
                next=headA;
                headA=null;
            } else if (comp.compare(headA,headB)<=0) {
                next=headA;
                headA=null;
            } else {
                next=headB;
                headB=null;
            }
            assert next!=null;
            Preconditions.checkArgument(result.isEmpty() || comp.compare(result.get(result.size()-1),next)<=0,
                    "The input collections are not sorted");
            result.add(next);
            if (headA==null) headA=iteratorA.hasNext()?iteratorA.next():null;
            if (headB==null) headB=iteratorB.hasNext()?iteratorB.next():null;
        }
        return result;
    }



}
