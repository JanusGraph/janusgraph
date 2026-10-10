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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.es.compat.AbstractESCompat;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
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
 * the offset leaves nothing to ask for, is the same search read in pages: through a scroll, or through a point in time
 * and {@code search_after} where the {@link ElasticSearchPagingMode} says so and the cluster has them. Either stops as
 * soon as the consumer does or the limit is reached, so that the point in time or the scroll context is released
 * instead of being left to expire.
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
    private final ElasticSearchPagingMode pagingMode;
    private final int adaptiveMaxShards;
    private final int adaptiveMaxPageSize;

    /**
     * @param pageSize            the size of the first request of a result which no limit bounds, and of every page
     *                            read after it
     * @param pagingMode          how a result larger than a page is read where the cluster can page through a point
     *                            in time
     * @param adaptiveMaxShards   the most shards of an index which
     *                            {@link ElasticSearchPagingMode#ADAPTIVE_POINT_IN_TIME} reads through a point in time
     * @param adaptiveMaxPageSize the largest page size with which
     *                            {@link ElasticSearchPagingMode#ADAPTIVE_POINT_IN_TIME} reads through a point in time
     */
    ElasticSearchSearcher(ElasticSearchClient client, AbstractESCompat compat, int pageSize,
                          ElasticSearchPagingMode pagingMode, int adaptiveMaxShards, int adaptiveMaxPageSize) {
        this.client = client;
        this.compat = compat;
        this.pageSize = pageSize;
        this.pagingMode = Preconditions.checkNotNull(pagingMode, "A paging mode is required");
        this.adaptiveMaxShards = adaptiveMaxShards;
        this.adaptiveMaxPageSize = adaptiveMaxPageSize;
    }

    /**
     * @param request    the search, whose size and offset are decided here
     * @param parameters further parameters of the request body, or null
     * @param offset     hits to skip
     * @param limit      hits wanted after the offset, {@link Query#NO_LIMIT} for all of them
     * @return the hits after the offset, at most the limit of them. Closing the stream releases the point in time or
     * scroll context of a consumer which stops before the end
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
        //The shards the search runs on, as the first request reports them; 0 without one
        int shards = 0;
        if (probeSize > 0) {
            request.setFrom(offset);
            request.setSize(probeSize);
            final ElasticSearchResponse response = search(indexStoreName, request, parameters, false);
            if (response.numResults() < probeSize) {
                return limit(response.getResults(), limit);
            }
            shards = response.getTotalShards();
        }
        //The pages start at the first hit, so the offset is skipped here
        request.setFrom(0);
        request.setSize(pageSize);
        final long pagesLimit = scrollLimit(offset, limit);
        final boolean unbounded = limit == Query.NO_LIMIT;
        final boolean pointInTimeMayRead = pagingMode == ElasticSearchPagingMode.POINT_IN_TIME
            || (pagingMode == ElasticSearchPagingMode.ADAPTIVE_POINT_IN_TIME && mayKeepUp(request, unbounded, shards));
        //The body tells whether the pages have a sort of their own, which leaves ADAPTIVE_POINT_IN_TIME to a scroll, so
        //it is built only where a point in time may read them, and a point in time which reads them takes it
        final Map<String, Object> requestBody = pointInTimeMayRead && client.supportsPointInTime()
            ? compat.createRequestBody(request, parameters) : null;
        final Stream<RawQuery.Result<String>> pages = requestBody != null
                && (pagingMode == ElasticSearchPagingMode.POINT_IN_TIME || !hasSort(requestBody))
            ? pointInTime(indexStoreName, request, requestBody, pagesLimit, unbounded)
            : scroll(indexStoreName, request, parameters, pagesLimit);
        return limit(pages.skip(offset), limit);
    }

    //Whether ADAPTIVE_POINT_IN_TIME may read the pages through a point in time, which is about as fast as a scroll or
    //faster where it reads them in the order of the index: the query has no limit and doesn't want its hits by
    //relevance, on an index of few shards (the first request has to have told them) and in small pages; then the pages
    //must have no sort of their own. In any other order, through more shards or in larger pages, a point in time pages
    //more slowly than a scroll
    private boolean mayKeepUp(ElasticSearchRequest request, boolean unbounded, int shards) {
        return unbounded && !request.isRelevanceOrdered()
            && shards > 0 && shards <= adaptiveMaxShards && pageSize <= adaptiveMaxPageSize;
    }

    //A sort parameter may be a list, a field name or an object; only none, or an empty list, is no sort
    private static boolean hasSort(Map<String, Object> requestBody) {
        final Object sort = requestBody.get("sort");
        return sort != null && !(sort instanceof Collection && ((Collection<?>) sort).isEmpty());
    }

    private Stream<RawQuery.Result<String>> scroll(String indexStoreName, ElasticSearchRequest request,
                                                   Parameter[] parameters, long limit) throws IOException {
        final ElasticSearchResponse firstPage = search(indexStoreName, request, parameters, true);
        final ElasticSearchScroll scroll = new ElasticSearchScroll(client, firstPage, pageSize, limit);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scroll, Spliterator.ORDERED), false)
            .onClose(scroll::close);
    }

    /**
     * Reads the pages through a point in time, which holds the state of the index so that the pages agree, each asked
     * for after the last hit of the one before. The pages need a sort. A request which has none, neither of its own
     * nor as a parameter, is sorted by score, the order of a single request, so that the pages of a limited query
     * come in the order its single request would have: a limited query may be executed again with a larger limit,
     * skipping the hits delivered so far, which needs the same order each time. A raw query's hits are wanted by score
     * either way. A query without a limit is executed once, and its pages are sorted by {@code _shard_doc}, the order
     * of the index, which Elasticsearch pages through without scoring, where a sort by score costs every page the
     * scoring of the whole result. Elasticsearch adds the {@code _shard_doc} tiebreaker to every sort of a search of a
     * point in time, so the sort values of a hit identify it, and the sort by score with that tiebreaker is the order
     * of a single request. No page counts the total number of hits, which a scroll can't do without.
     *
     * @param unbounded whether the query has no limit, so that it is never executed again with a larger one
     */
    private Stream<RawQuery.Result<String>> pointInTime(String indexStoreName, ElasticSearchRequest request,
                                                        Map<String, Object> requestBody, long limit, boolean unbounded)
            throws IOException {
        final String pitId = client.openPointInTime(indexStoreName);
        try {
            requestBody.put(TRACK_TOTAL_HITS_PARAMETER, false);
            //search_after takes the place of the offset
            requestBody.remove("from");
            if (!hasSort(requestBody)) {
                final boolean byScore = request.isRelevanceOrdered() || !unbounded;
                requestBody.put("sort", ImmutableList.of(byScore
                    ? ImmutableMap.of("_score", "desc") : ImmutableMap.of("_shard_doc", "asc")));
            }
            final ElasticSearchResponse firstPage = client.searchPointInTime(pitId, requestBody);
            log.debug("Executed search of size {} in {} ms", request.getSize(), firstPage.getTook());
            final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, pitId, requestBody, firstPage,
                pageSize, limit);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(pages, Spliterator.ORDERED), false)
                .onClose(pages::close);
        } catch (IOException | RuntimeException e) {
            try {
                client.closePointInTime(pitId);
            } catch (IOException | RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
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
