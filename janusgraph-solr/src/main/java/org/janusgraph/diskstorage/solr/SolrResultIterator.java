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

package org.janusgraph.diskstorage.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * @author David Clement (david.clement90@laposte.net)
 */
class SolrResultIterator<E> implements Iterator<E> {

    private final SolrClient solrClient;
    private final BlockingQueue<E> queue;
    private int count;
    private int numBatches;
    private final Long limit;
    private final int offset;
    private final int batchSize;
    private final String collection;
    private final SolrQuery solrQuery;
    private final Function<SolrDocument, E> getFieldValue;

    public SolrResultIterator(SolrClient solrClient, Integer limit, int offset, int nbDocByQuery, String collection, SolrQuery solrQuery, Function<SolrDocument, E> function) throws SolrServerException, IOException {
        this.solrClient = solrClient;
        count = 0;
        this.offset = offset;
        this.batchSize = nbDocByQuery;
        queue = new LinkedBlockingQueue<>();
        this.collection = collection;
        this.solrQuery = solrQuery;
        this.getFieldValue = function;
        final long nbFound = solrClient.queryAndStreamResponse(collection, solrQuery, new SolrCallbackHandler(this, function)).getResults().getNumFound() - offset;
        this.limit = limit != null ? Math.min(nbFound, limit) : nbFound;
        numBatches = 1;
    }

    public BlockingQueue<E> getQueue(){
        return queue;
    }

    @Override
    public boolean hasNext() {
        if (count != 0 && count % batchSize == 0 && count < limit) {
            try {
                solrQuery.setStart(numBatches * batchSize + offset);
                solrClient.queryAndStreamResponse(collection, solrQuery, new SolrCallbackHandler(this, getFieldValue));
                numBatches++;
            } catch (final SolrServerException e) {
                throw new UncheckedSolrException(e.getMessage(), e);
            } catch (final IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
        return count < limit;
    }

    @Override
    public E next() {
        try {
            count++;
            return queue.take();
        } catch (final InterruptedException e) {
             throw new UncheckedIOException(new IOException("Interrupted waiting on queue", e));
        }
    }

    private static class SolrCallbackHandler<E> extends StreamingResponseCallback {

        private final SolrResultIterator<E> iterator;
        private final Function<SolrDocument, E> function;

        public SolrCallbackHandler(SolrResultIterator<E> iterator, Function<SolrDocument, E> function) {
            this.function = function;
            this.iterator = iterator;
        }

        @Override
        public void streamDocListInfo(long nbFound, long start, Float aMaxScore) {
        }

        @Override
        public void streamSolrDocument(SolrDocument doc) {
            iterator.getQueue().add(function.apply(doc));
        }
    }
}
