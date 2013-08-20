package com.thinkaurelius.titan.util.datastructures;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

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



}
