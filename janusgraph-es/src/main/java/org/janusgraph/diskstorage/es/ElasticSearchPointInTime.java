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

package org.janusgraph.diskstorage.es;

import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.RawQuery.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Iterates over the hits of a search of a point in time one page at a time, each page asked for after the last hit of
 * the one before ({@code search_after}), and closes the point in time as soon as it can: when a page comes back short,
 * when the limit is within the pages received, or when the consumer closes it.
 */
public class ElasticSearchPointInTime implements Iterator<RawQuery.Result<String>>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchPointInTime.class);

    private static final String SEARCH_AFTER = "search_after";

    private final Queue<RawQuery.Result<String>> queue;
    private final ElasticSearchClient client;
    private final Map<String, Object> requestBody;
    private final int pageSize;
    private final long limit;
    private long received;
    private long delivered;
    private boolean isFinished;
    private boolean released;
    private String pitId;
    private List<Object> lastSort;

    /**
     * @param requestBody the body of the search, which every page sends again with the position of the one before;
     *                    it is kept and changed here
     * @param firstPage   the response to the first search of the point in time with that body
     * @param limit       how many hits the consumer wants at most, {@link Long#MAX_VALUE} for all of them. Pages
     *                    beyond the limit are never requested, and the point in time is closed once the limit is
     *                    within those received
     */
    public ElasticSearchPointInTime(ElasticSearchClient client, String pitId, Map<String, Object> requestBody,
                                    ElasticSearchResponse firstPage, int pageSize, long limit) {
        this.queue = new ArrayDeque<>();
        this.client = client;
        this.pitId = pitId;
        this.requestBody = requestBody;
        this.pageSize = pageSize;
        this.limit = limit;
        update(firstPage);
    }

    private void update(ElasticSearchResponse response) {
        response.getResults().forEach(queue::add);
        received += response.numResults();
        if (response.getPitId() != null) {
            pitId = response.getPitId();
        }
        lastSort = response.getLastSort();
        //A short page is the last one; once the limit is within the pages received nothing beyond them is wanted; and
        //a page whose last hit has no sort values leaves nothing to search after
        isFinished = response.numResults() < pageSize || received >= limit || lastSort == null;
        if (isFinished) {
            release();
        }
    }

    @Override
    public boolean hasNext() {
        if (delivered >= limit) {
            return false;
        }
        if (!queue.isEmpty()) {
            return true;
        }
        if (isFinished) {
            return false;
        }
        try {
            requestBody.put(SEARCH_AFTER, lastSort);
            final ElasticSearchResponse response = client.searchPointInTime(pitId, requestBody);
            update(response);
            return response.numResults() > 0;
        } catch (final IOException e) {
            //The pages can't go on from here, so the point in time has served its purpose
            finishAfterAFailedPage();
            throw new UncheckedIOException("Could not read the next page of point in time " + pitId, e);
        } catch (final RuntimeException e) {
            finishAfterAFailedPage();
            throw e;
        }
    }

    private void finishAfterAFailedPage() {
        isFinished = true;
        release();
    }

    @Override
    public Result<String> next() {
        if (hasNext()) {
            delivered++;
            return queue.remove();
        }
        throw new NoSuchElementException();
    }

    /**
     * Ends the iteration and closes the point in time. For a consumer which stops before the end; the point in time
     * of one which reaches it is closed by then.
     */
    @Override
    public void close() {
        isFinished = true;
        queue.clear();
        release();
    }

    //Best effort: the point in time expires after its keep-alive on its own, so failing to close it must not fail a
    //search whose hits are complete
    private void release() {
        if (released || pitId == null) {
            return;
        }
        released = true;
        try {
            client.closePointInTime(pitId);
        } catch (IOException | RuntimeException e) {
            log.debug("Could not close the Elasticsearch point in time {}, which expires on its own", pitId, e);
        }
    }
}
