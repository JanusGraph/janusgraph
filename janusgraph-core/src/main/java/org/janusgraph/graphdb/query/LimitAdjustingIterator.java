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

package org.janusgraph.graphdb.query;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator implementation that wraps around another iterator to iterate it up to a given limit.
 * The idea is that the wrapped iterator is based on data that is fairly expensive to retrieve (e.g. from a database).
 * As such, we don't want to retrieve all of it but "just enough". However, if more data is requested, then we want
 * the wrapped iterator to be updated (i.e. additional data be retrieved).
 * <p>
 * The limit for the wrapped iterator is updated by a factor of 2. When the iterator is updated, the iterator must be
 * iterated through to the point of the last returned element. While this may seem expensive, it is less expensive than
 * retrieving more than needed elements in the first place. However, this still means the initial currentLimit in the
 * constructor should be chosen wisely.
 * <p>
 * A wrapped iterator which comes to its end with fewer elements than the limit tells that the backend ran out, unless
 * it leaves some of the backend's results out and the backend returned as many as the limit allowed
 * ({@link BackendLimitAware}): then the limit is updated as well. Such an iterator also tells how many of the backend's
 * results it went through, and the iterator under the updated limit skips as many of them before it turns any into an
 * element, rather than as many elements as were returned: a result which turned into no element between the reads, as
 * a relation which the transaction removed meanwhile, would otherwise shift the elements, and every result before
 * would be turned into an element again. Any other iterator is iterated through as many elements as were returned.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LimitAdjustingIterator<R> implements CloseableIterator<R> {

    private final int maxLimit;
    private int currentLimit;
    private int count;

    private Iterator<R> iterator;

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
        this.iterator = null;
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
        if (iterator ==null) iterator = getNewIterator(currentLimit);
        if (currentLimit == Query.NO_LIMIT || count < currentLimit) {
            if (iterator.hasNext()) return true;
            //The wrapped iterator came to its end before the limit, which tells that the backend ran out, unless the
            //iterator left some of the backend's results out while the backend returned as many as the limit allowed
            if (currentLimit == Query.NO_LIMIT || !(iterator instanceof BackendLimitAware)
                    || !((BackendLimitAware) iterator).hasBackendReachedLimit()) {
                return false;
            }
        }
        if (currentLimit>=maxLimit) return false;

        //The old iterator found no element here which it didn't return: it came to its end, or the element it returned
        //last filled the limit
        final long backendResultsRead = iterator instanceof BackendLimitAware
            ? ((BackendLimitAware) iterator).getBackendResultsRead() : -1;
        //Close old iterator. This is needed otherwise it would not be timed properly by profiler.
        CloseableIterator.closeIterator(iterator);
        //Get an iterator with an updated limit
        currentLimit = (int) Math.min(maxLimit, Math.round(currentLimit * 2.0));
        iterator = getNewIterator(currentLimit);

        if (backendResultsRead >= 0 && iterator instanceof BackendLimitAware) {
            //How far into the backend's results the old iterator got, which no removal since then moves
            ((BackendLimitAware) iterator).skipBackendResults(backendResultsRead);
        } else {
            /*
            We need to iterate out the iterator to the point where we last left of. This is pretty expensive and hence
            it should be ensured that the initial limit is a good guesstimate.
             */
            for (int i = 0; i < count; i++)
                iterator.next();
        }

        assert count < currentLimit : count + " vs " + currentLimit + " | " + maxLimit;
        return hasNext();
    }

    @Override
    public R next() {
        if (!hasNext())
            throw new NoSuchElementException();

        count++;
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(){
        CloseableIterator.closeIterator(iterator);
    }
}
