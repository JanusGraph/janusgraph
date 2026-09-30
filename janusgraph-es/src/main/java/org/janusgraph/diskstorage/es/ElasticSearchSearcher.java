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

import com.google.common.annotations.VisibleForTesting;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.es.compat.AbstractESCompat;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Fetches the hits of a search in as few requests as the result allows.
 * <p>
 * A result which a limit bounds within {@link #MAX_SEARCH_SIZE} comes back in one request of exactly that size,
 * with the offset applied by Elasticsearch and without counting the total. Otherwise the first request asks for one
 * hit more than a page after the offset, or for what is left up to {@link #MAX_SEARCH_SIZE} if that is less: when
 * fewer come back, that is the whole result and no scroll context is opened. Only when it comes back full, or when
 * the offset leaves nothing to ask for, does a scroll over the same search take its place, and that scroll stops as
 * soon as the consumer does or the limit is reached, so that its context is released instead of being left to expire.
 */
class ElasticSearchSearcher {

    /**
     * The most hits one request may return: Elasticsearch's default {@code index.max_result_window}, which bounds
     * {@code from + size}. JanusGraph lifts it on the indexes it creates, but not on external ones, so the default is
     * what every index is known to accept.
     */
    static final int MAX_SEARCH_SIZE = 10_000;

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchSearcher.class);
    private static final String TRACK_TOTAL_HITS_PARAMETER = "track_total_hits";

    private final ElasticSearchClient client;
    private final AbstractESCompat compat;
    private final int pageSize;

    /**
     * @param pageSize the size of the first request of a result which no limit bounds, and of every page of a scroll
     */
    ElasticSearchSearcher(ElasticSearchClient client, AbstractESCompat compat, int pageSize) {
        this.client = client;
        this.compat = compat;
        this.pageSize = pageSize;
    }

    /**
     * @param request    the search, whose size and offset are decided here
     * @param parameters further parameters of the request body, or null
     * @param offset     hits to skip
     * @param limit      hits wanted after the offset, {@link Query#NO_LIMIT} for all of them
     * @return the hits after the offset, at most the limit of them. Closing the stream releases the scroll context
     * of a consumer which stops before the end
     */
    Stream<RawQuery.Result<String>> search(String indexStoreName, ElasticSearchRequest request, Parameter[] parameters,
                                           int offset, int limit) throws IOException {
        if (limit == 0) {
            return Stream.empty();
        }
        if (limit != Query.NO_LIMIT && (long) offset + limit <= MAX_SEARCH_SIZE) {
            request.setFrom(offset);
            request.setSize(limit);
            return search(indexStoreName, request, parameters, false).getResults();
        }
        //One hit more than a page after the offset, but never beyond the last hit a request may reach, tells whether
        //what comes back is the whole result: it is when fewer come back than were asked for
        final int probeSize = (int) Math.min(pageSize + 1L, MAX_SEARCH_SIZE - (long) offset);
        if (probeSize > 0) {
            request.setFrom(offset);
            request.setSize(probeSize);
            final ElasticSearchResponse response = search(indexStoreName, request, parameters, false);
            if (response.numResults() < probeSize) {
                return limit(response.getResults(), limit);
            }
        }
        //A scroll starts at the first hit, so the offset is skipped here
        request.setFrom(0);
        request.setSize(pageSize);
        final ElasticSearchResponse firstPage = search(indexStoreName, request, parameters, true);
        final ElasticSearchScroll scroll = new ElasticSearchScroll(client, firstPage, pageSize, scrollLimit(offset, limit));
        return limit(StreamSupport.stream(Spliterators.spliteratorUnknownSize(scroll, Spliterator.ORDERED), false)
            .onClose(scroll::close)
            .skip(offset), limit);
    }

    private ElasticSearchResponse search(String indexStoreName, ElasticSearchRequest request, Parameter[] parameters,
                                         boolean useScroll) throws IOException {
        final Map<String, Object> requestBody = compat.createRequestBody(request, parameters);
        //A scroll has to count its total, Elasticsearch rejects switching that off; nothing else here needs the count
        if (!useScroll) {
            requestBody.put(TRACK_TOTAL_HITS_PARAMETER, false);
        }
        final ElasticSearchResponse response = client.search(indexStoreName, requestBody, useScroll);
        log.debug("Executed search of size {} in {} ms", request.getSize(), response.getTook());
        return response;
    }

    //The offset is skipped on the client, so the scroll has to hand out that many hits more than the limit, which
    //a long always represents
    @VisibleForTesting
    static long scrollLimit(int offset, int limit) {
        return limit == Query.NO_LIMIT ? Long.MAX_VALUE : (long) offset + limit;
    }

    private static Stream<RawQuery.Result<String>> limit(Stream<RawQuery.Result<String>> hits, int limit) {
        return limit == Query.NO_LIMIT ? hits : hits.limit(limit);
    }
}
