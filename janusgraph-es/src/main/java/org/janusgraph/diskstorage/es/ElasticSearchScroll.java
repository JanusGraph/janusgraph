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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
public class ElasticSearchScroll implements Iterator<RawQuery.Result<String>> {

    private final BlockingQueue<RawQuery.Result<String>> queue;
    private final ElasticSearchClient client;
    private final int batchSize;

    private boolean isFinished;
    private String scrollId;

    public ElasticSearchScroll(ElasticSearchClient client, ElasticSearchResponse initialResponse, int nbDocByQuery) {
        queue = new LinkedBlockingQueue<>();
        this.client = client;
        this.batchSize = nbDocByQuery;
        update(initialResponse);
    }

    private void update(ElasticSearchResponse response) {
        response.getResults().forEach(queue::add);
        this.scrollId = response.getScrollId();
        this.isFinished = response.numResults() < this.batchSize;
        try {
            if (isFinished) client.deleteScroll(scrollId);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (!queue.isEmpty()) {
                return true;
            }
            if (isFinished) {
                return false;
            }
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
            return queue.remove();
        }
        throw new NoSuchElementException();
    }
}
