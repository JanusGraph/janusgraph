package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanElement;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wraps around a result set iterator to return up to the specified limit number of elements
 * and implement the {@link java.util.Iterator#remove()} method based on element's remove method.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ResultSetIterator<R extends TitanElement> implements Iterator<R> {

    private final Iterator<R> iter;
    private final int limit;

    private R current;
    private R next;
    private int count;


    public ResultSetIterator(Iterator<R> inner, int limit) {
        this.iter = inner;
        this.limit = limit;
        count = 0;

        this.current = null;
        this.next = nextInternal();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    private R nextInternal() {
        R r = null;
        if (count < limit && iter.hasNext()) {
            r = iter.next();
        }
        return r;
    }

    @Override
    public R next() {
        if (!hasNext())
            throw new NoSuchElementException();

        current = next;
        count++;
        next = nextInternal();
        return current;
    }

    @Override
    public void remove() {
        if (current != null)
            current.remove();
        else
            throw new UnsupportedOperationException();
    }

    public static<R extends TitanElement> Iterable<R> wrap(final Iterable<R> inner, final int limit) {
        return new Iterable<R>() {

            @Override
            public Iterator<R> iterator() {
                return new ResultSetIterator<R>(inner.iterator(),limit);
            }
        };
    }

}
