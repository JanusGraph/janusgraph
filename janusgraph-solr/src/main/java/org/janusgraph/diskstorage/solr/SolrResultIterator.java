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
import org.apache.solr.common.params.CommonParams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * The results of a query, read from Solr a page at a time, each page at its offset, for which Solr collects and sorts
 * every result before it again. The first page holds the query's rows, and every page after it twice as many results
 * as the page before, up to the largest page, but never more than the limit leaves, nor than the first page found after
 * the query's start: a large result takes few pages, a read which stops early asks for no more than twice the results
 * it takes, plus a first page, and results which other transactions add meanwhile can't keep a read going.
 *
 * @author David Clement (david.clement90@laposte.net)
 */
class SolrResultIterator<E> implements Iterator<E> {

    private final SolrClient solrClient;
    private final BlockingQueue<E> queue;
    private int count;
    // The results which the read takes at most: the limit, and those which its first page found after the query's start
    private final long limit;
    private final int start;
    private final int maxRows;
    private int rows;
    private final String collection;
    private final SolrQuery solrQuery;
    private final Function<SolrDocument, E> getFieldValue;
    private boolean lastPage;

    /**
     * @param maxRows the largest page; one no larger than the query's rows reads every page in pages of that size
     */
    public SolrResultIterator(SolrClient solrClient, Integer limit, int maxRows, String collection,
                              SolrQuery solrQuery, Function<SolrDocument, E> function)
            throws SolrServerException, IOException {
        this.solrClient = solrClient;
        count = 0;
        start = solrQuery.getStart() != null ? solrQuery.getStart() : 0;
        rows = solrQuery.getRows();
        this.maxRows = Math.max(maxRows, rows);
        queue = new LinkedBlockingQueue<>();
        this.collection = collection;
        this.solrQuery = solrQuery;
        this.getFieldValue = function;
        final long found = readPage();
        // The read takes no more results than its first page found after the query's start, so that results which other
        // transactions add meanwhile can't keep it going; but the count of a request which sets minExactCount, as a
        // direct index query may, can be a lower bound, which bounds no read
        final long first = solrQuery.get(CommonParams.MIN_EXACT_COUNT) == null ? found - start : Long.MAX_VALUE;
        this.limit = Math.min(limit != null ? limit : Long.MAX_VALUE, first);
    }

    public BlockingQueue<E> getQueue(){
        return queue;
    }

    @Override
    public boolean hasNext() {
        if (queue.isEmpty() && !lastPage && count < limit) {
            rows = (int) Math.min(Math.min(2L * rows, maxRows), limit - count);
            try {
                readPage();
            } catch (final SolrServerException e) {
                throw new UncheckedSolrException(e.getMessage(), e);
            } catch (final IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
        return count < limit && !queue.isEmpty();
    }

    //Reads the page of the given rows after the results read so far into the queue, which they have left empty, and
    //returns the count of the results which Solr found. The page is the last one where it comes back empty, whatever
    //its count, so that every read ends, or where Solr found no more results than those up to its end, as it says in
    //the page's own count, as where other transactions deleted results since the first page. A page which comes back
    //short isn't the last one where the count says that more remain: a distributed request leaves out a result which
    //was deleted between its phases, and takes it off the count. A count which isn't exact, as where a request sets
    //minExactCount, is a lower bound past the end of the page, as Solr counts exactly up to there at least, so it ends
    //no read
    private long readPage() throws SolrServerException, IOException {
        solrQuery.setStart(start + count);
        solrQuery.setRows(rows);
        final long found = solrClient.queryAndStreamResponse(collection, solrQuery,
            new SolrCallbackHandler(this, getFieldValue)).getResults().getNumFound();
        lastPage = queue.isEmpty() || found <= start + count + queue.size();
        return found;
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
