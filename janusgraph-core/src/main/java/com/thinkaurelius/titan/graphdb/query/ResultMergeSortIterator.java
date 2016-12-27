package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ResultMergeSortIterator<R> implements Iterator<R> {


    private final Iterator<R> first;
    private final Iterator<R> second;
    private final Comparator<R> comp;
    private final boolean filterDuplicates;

    private R nextFirst;
    private R nextSecond;
    private R next;

    public ResultMergeSortIterator(Iterator<R> first, Iterator<R> second, Comparator<R> comparator, boolean filterDuplicates) {
        Preconditions.checkNotNull(first);
        Preconditions.checkNotNull(second);
        Preconditions.checkNotNull(comparator);
        this.first = first;
        this.second = second;
        this.comp = comparator;
        this.filterDuplicates = filterDuplicates;

        nextFirst = null;
        nextSecond = null;
        next = nextInternal();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public R next() {
        if (!hasNext()) throw new NoSuchElementException();
        R current = next;
        next = null;
        do {
            next = nextInternal();
        } while (next != null && filterDuplicates && comp.compare(current, next) == 0);
        return current;
    }

    public R nextInternal() {
        if (nextFirst == null && first.hasNext()) {
            nextFirst = first.next();
            assert nextFirst != null;
        }
        if (nextSecond == null && second.hasNext()) {
            nextSecond = second.next();
            assert nextSecond != null;
        }
        R result = null;
        if (nextFirst == null && nextSecond == null) {
            return null;
        } else if (nextFirst == null) {
            result = nextSecond;
            nextSecond = null;
        } else if (nextSecond == null) {
            result = nextFirst;
            nextFirst = null;
        } else {
            //Compare
            int c = comp.compare(nextFirst, nextSecond);
            if (c <= 0) {
                result = nextFirst;
                nextFirst = null;
            } else {
                result = nextSecond;
                nextSecond = null;
            }
        }
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static<R> Iterable<R> mergeSort(final Iterable<R> first, final Iterable<R> second,
                                           final Comparator<R> comparator, final boolean filterDuplicates) {
        return new Iterable<R>() {
            @Override
            public Iterator<R> iterator() {
                return new ResultMergeSortIterator<R>(first.iterator(),second.iterator(),comparator,filterDuplicates);
            }
        };
    }



}
