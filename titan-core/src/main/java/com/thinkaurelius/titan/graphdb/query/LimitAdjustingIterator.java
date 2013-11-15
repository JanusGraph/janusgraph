package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LimitAdjustingIterator<R> implements Iterator<R> {

    private final int maxLimit;
    private int currentLimit;
    private int count;

    private Iterator<R> iter;


    public LimitAdjustingIterator(final int maxLimit, final int currentLimit) {
        Preconditions.checkArgument(currentLimit>0 && maxLimit>0,"Invalid limits: current [%s], max [%s]",currentLimit,maxLimit);
        this.currentLimit = currentLimit;
        this.maxLimit = maxLimit;
        this.count = 0;
        this.iter = null;
    }

    public abstract Iterator<R> getNewIterator(int newLimit);

    @Override
    public boolean hasNext() {
        if (iter==null) iter = getNewIterator(currentLimit);
        if (count < currentLimit)
            return iter.hasNext();
        if (currentLimit>=maxLimit) return false;

        //Update query and iterate through
        currentLimit = (int) Math.min(maxLimit, Math.round(currentLimit * 2.0));
        iter = getNewIterator(currentLimit);

        // TODO: this is very-very bad, we at least should try to do that in parallel
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
