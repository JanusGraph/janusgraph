// Copyright 2026 JanusGraph Authors
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

/**
 * An iterator over what a backend returned for a read under a limit, which leaves some of the backend's results out,
 * such as the ids of relations which the transaction removed. It can then come to its end before that limit although
 * the backend holds more results, which {@link LimitAdjustingIterator} has to tell apart from a backend which ran out.
 * And the elements it returns don't tell how far into the backend's results it got, which a read under a larger limit
 * has to skip, so it tells that too.
 */
public interface BackendLimitAware {

    /**
     * @return whether the backend returned as many results as the limit of the read allowed, so that it may hold more,
     * as of {@link #getBackendResultsRead()}
     */
    boolean hasBackendReachedLimit();

    /**
     * @return how many of the backend's results this iterator went through, those it left out and those it skipped
     * included, up to the last element it found, or up to its end once it came to it. It finds an element when
     * {@code hasNext} looks for one, ahead of returning it. Closing the iterator doesn't change the count, even where
     * it reads on. To tell this, the iterator takes the backend's results one at a time, however the backend hands
     * them out
     */
    long getBackendResultsRead();

    /**
     * Skips the first results of the backend, without turning any of them into an element. Called before the iterator
     * looks for its first element, that is before the first call of {@code hasNext} or {@code next}.
     *
     * @param count how many of the backend's results to skip, at least 0
     */
    void skipBackendResults(long count);
}
