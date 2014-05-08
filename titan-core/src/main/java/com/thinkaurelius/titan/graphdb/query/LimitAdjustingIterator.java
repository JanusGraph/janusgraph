package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator implementation that wraps around another iterator to iterate it up to a given limit.
 * The idea is that the wrapped iterator is based on data that is fairly expensive to retrieve (e.g. from a database).
 * As such, we don't want to retrieve all of it but "just enough". However, if more data is requested, then we want
 * the wrapped iterator to be updated (i.e. additional data be retrieved).
 * </p>
 * The limit for the wrapped iterator is updated by a factor of 2. When the iterator is updated, the iterator must be
 * iterated through to the point of the last returned element. While this may seem expensive, it is less expensive than
 * retrieving more than needed elements in the first place. However, this still means the initial currentLimit in the
 * constructor should be chosen wisely.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LimitAdjustingIterator<R> implements Iterator<R> {

    private final int maxLimit;
    private int currentLimit;
    private int count;

    private Iterator<R> iter;

    /**
     * Initializes this iterator with the current limit and the maximum number of elements that may be retrieved from the
     * wrapped iterator.
     *
     * @param maxLimit
     * @param currentLimit
     */
    public LimitAdjustingIterator(final int maxLimit, final int currentLimit) {
        Preconditions.checkArgument(currentLimit>0 && maxLimit>0,"Invalid limits: current [%s], max [%s]",currentLimit,maxLimit);
        this.currentLimit = currentLimit;
        this.maxLimit = maxLimit;
        this.count = 0;
        this.iter = null;
    }

    /**
     * This returns the wrapped iterator with up to the specified number of elements.
     *
     * @param newLimit
     * @return
     */
    public abstract Iterator<R> getNewIterator(int newLimit);

    @Override
    public boolean hasNext() {
        if (iter==null) iter = getNewIterator(currentLimit);
        if (count < currentLimit)
            return iter.hasNext();
        if (currentLimit>=maxLimit) return false;

        //Get an iterator with an updated limit
        currentLimit = (int) Math.min(maxLimit, Math.round(currentLimit * 2.0));
        iter = getNewIterator(currentLimit);

        /*
        We need to iterate out the iterator to the point where we last left of. This is pretty expensive and hence
        it should be ensured that the initial limit is a good guesstimate.
         */
        for (int i = 0; i < count; i++)
            iter.next();

        assert count < currentLimit : count + " vs " + currentLimit + " | " + maxLimit;
        return hasNext();
    }

    @Override
    public R next() {
        if (!hasNext())
            throw new NoSuchElementException();

        count++;
        return iter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
