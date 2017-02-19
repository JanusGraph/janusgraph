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
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Utility class for interacting with {@link Iterable}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IterablesUtil {

    public static final <O> Iterable<O> emptyIterable() {
        return new Iterable<O>() {

            @Override
            public Iterator<O> iterator() {
                return Iterators.emptyIterator();
            }

        };
    }

    public static final Predicate NO_FILTER = new NoFilter();

    public static final <E> Predicate<E> noFilter() {
        return (Predicate<E>)NO_FILTER;
    }

    private static class NoFilter<E> implements Predicate<E> {

        @Override
        public boolean apply(@Nullable E e) {
            return true;
        }
    }

    public static final<O> Iterable<O> limitedIterable(final Iterable<O> iterable, final int limit) {
        return Iterables.filter(iterable,new Predicate<O>() {

            int count = 0;

            @Override
            public boolean apply(@Nullable O o) {
                count++;
                return count<=limit;
            }
        });
    }

    public static final int size(Iterable i) {
        if (i instanceof Collection) return ((Collection)i).size();
        else return Iterables.size(i);
    }

    public static final boolean sizeLargerOrEqualThan(Iterable i, int limit) {
        if (i instanceof Collection) return ((Collection)i).size()>=limit;
        Iterator iter = i.iterator();
        int count=0;
        while (iter.hasNext()) {
            iter.next();
            count++;
            if (count>=limit) return true;
        }
        return false;
    }

    public static<E> List<E> mergeSort(Collection<E> a, Collection<E> b, Comparator<E> comp) {
        Iterator<E> itera = a.iterator(), iterb = b.iterator();
        E heada = itera.hasNext()?itera.next():null;
        E headb = iterb.hasNext()?iterb.next():null;
        List<E> result = new ArrayList<>(a.size()+b.size());
        while (heada!=null || headb!=null) {
            E next;
            if (heada==null) {
                next=headb;
                headb = null;
            } else if (headb==null) {
                next=heada;
                heada=null;
            } else if (comp.compare(heada,headb)<=0) {
                next=heada;
                heada=null;
            } else {
                next=headb;
                headb=null;
            }
            assert next!=null;
            Preconditions.checkArgument(result.isEmpty() || comp.compare(result.get(result.size()-1),next)<=0,
                    "The input collections are not sorted");
            result.add(next);
            if (heada==null) heada=itera.hasNext()?itera.next():null;
            if (headb==null) headb=iterb.hasNext()?iterb.next():null;
        }
        return result;
    }



}
