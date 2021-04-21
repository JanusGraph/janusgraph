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

import org.janusgraph.diskstorage.indexing.RawQuery.Result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class ElasticSearchSearchAfter implements Iterator<Result<String>> {

    private final BlockingDeque<Result<String>> queue;
    private final ElasticSearchClient client;
    private final Map<String, Object> requestData;
    private final int batchSize;

    private boolean isFinished;
    private String pitId;
    private List<Object> searchAfter;

    public ElasticSearchSearchAfter(ElasticSearchClient client, String pitId, ElasticSearchResponse initialResponse, Map<String,Object> requestData, int nbDocByQuery) {
        queue = new LinkedBlockingDeque<>();
        this.client = client;
        this.pitId = pitId;
        this.requestData = requestData;
        this.batchSize = nbDocByQuery;
        update(initialResponse);
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

            final ElasticSearchResponse res = client.searchAfterWithPit(pitId, requestData, searchAfter);
            update(res);
            return res.numResults() > 0;
        } catch (final IOException e) {
             throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    private void update(ElasticSearchResponse response) {
        response.getResults().forEach(queue::add);
        this.searchAfter = queue.isEmpty() ? null : queue.peekLast().getSort();
        this.pitId = response.getPitId();
        this.isFinished = response.numResults() < this.batchSize;
        try {
            if (isFinished) client.deletePit(pitId);
        } catch (IOException e) {
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
