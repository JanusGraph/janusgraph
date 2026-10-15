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

package org.janusgraph.diskstorage.es;

import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.RawQuery.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Iterates over the hits of a scroll one page at a time, and releases the scroll context as soon as it can:
 * when a page comes back short, when the limit is within the pages received, or when the consumer closes it.
 *
 * @author David Clement (david.clement90@laposte.net)
 */
public class ElasticSearchScroll implements Iterator<RawQuery.Result<String>>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchScroll.class);

    private final Queue<RawQuery.Result<String>> queue;
    private final ElasticSearchClient client;
    private final int batchSize;
    private final long limit;

    private long received;
    private long delivered;
    private boolean isFinished;
    private boolean released;
    private String scrollId;

    public ElasticSearchScroll(ElasticSearchClient client, ElasticSearchResponse initialResponse, int nbDocByQuery) {
        this(client, initialResponse, nbDocByQuery, Long.MAX_VALUE);
    }

    /**
     * @param limit how many hits the consumer wants at most, {@link Long#MAX_VALUE} for all of them. Pages beyond
     *              the limit are never requested, and the context is released once the limit is within those received
     */
    public ElasticSearchScroll(ElasticSearchClient client, ElasticSearchResponse initialResponse, int nbDocByQuery,
                               long limit) {
        queue = new ArrayDeque<>();
        this.client = client;
        this.batchSize = nbDocByQuery;
        this.limit = limit;
        update(initialResponse);
    }

    private void update(ElasticSearchResponse response) {
        response.getResults().forEach(queue::add);
        received += response.numResults();
        this.scrollId = response.getScrollId();
        //A short page is the last one, and once the limit is within the pages received nothing beyond them is wanted
        this.isFinished = response.numResults() < this.batchSize || received >= limit;
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
            final ElasticSearchResponse res = client.search(scrollId);
            update(res);
            return res.numResults() > 0;
        } catch (final IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
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
     * Ends the iteration and releases the scroll context. For a consumer which stops before the end; the context
     * of one which reaches it is released by then.
     */
    @Override
    public void close() {
        isFinished = true;
        queue.clear();
        release();
    }

    //Best effort: the context expires after the scroll keep-alive on its own, so failing to release it must not
    //fail a search whose hits are complete
    private void release() {
        if (released || scrollId == null) {
            return;
        }
        released = true;
        try {
            client.deleteScroll(scrollId);
        } catch (IOException | RuntimeException e) {
            log.debug("Could not release the Elasticsearch scroll {}, which expires on its own", scrollId, e);
        }
    }
}
