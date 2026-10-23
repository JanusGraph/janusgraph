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

package org.janusgraph.diskstorage.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How a result is read in pages: each page at the offset of the results read before it, the first of the query's rows
 * and every one after it twice as large as the one before, up to the largest page, the limit and the results which the
 * first page found, until a page comes back empty or Solr found no more results than those up to its end. The client
 * answers from a list of ids, which a test changes between pages as other transactions change an index, and records
 * the offset and the rows of each request. An id which a test marks as lost between the phases of a request is left
 * out of its page and of its count, and deleted, as a distributed request leaves out a document deleted between its
 * phases. Where a request sets minExactCount, its count is exact up to minExactCount or to the end of the page,
 * whichever is larger, and a lower bound just past that beyond, as Solr counts
 */
public class SolrResultIteratorTest {

    private List<String> ids = new ArrayList<>();
    private final Set<String> lostBetweenPhases = new HashSet<>();
    private final List<List<Integer>> pages = new ArrayList<>();
    private Integer minExactCount;
    private SolrClient client;

    @BeforeEach
    public void setUp() throws Exception {
        client = mock(SolrClient.class);
        when(client.queryAndStreamResponse(anyString(), any(SolrParams.class), any(StreamingResponseCallback.class)))
            .thenAnswer(invocation -> {
                final SolrParams params = invocation.getArgument(1);
                final StreamingResponseCallback callback = invocation.getArgument(2);
                final int start = params.getInt(CommonParams.START);
                final int rows = params.getInt(CommonParams.ROWS);
                final Integer exactAtLeast = params.getInt(CommonParams.MIN_EXACT_COUNT);
                pages.add(Arrays.asList(start, rows));
                final List<String> page = new ArrayList<>(
                    ids.subList(Math.min(start, ids.size()), Math.min(start + rows, ids.size())));
                for (final String id : page) {
                    if (lostBetweenPhases.remove(id)) {
                        ids.remove(id);
                        continue;
                    }
                    final SolrDocument doc = new SolrDocument();
                    doc.setField("id", id);
                    callback.streamSolrDocument(doc);
                }
                final SolrDocumentList found = new SolrDocumentList();
                final long exactUpTo = exactAtLeast == null ? Long.MAX_VALUE : Math.max(exactAtLeast, start + rows);
                found.setNumFound(ids.size() <= exactUpTo ? ids.size() : exactUpTo + 1);
                final NamedList<Object> response = new NamedList<>();
                response.add("response", found);
                final QueryResponse queryResponse = new QueryResponse();
                queryResponse.setResponse(response);
                return queryResponse;
            });
    }

    private void index(int documents) {
        ids.clear();
        IntStream.range(0, documents).mapToObj(i -> "d" + i).forEach(ids::add);
    }

    private SolrResultIterator<String> read(Integer limit, int start, int rows, int maxRows) throws Exception {
        final SolrQuery query = new SolrQuery("*:*");
        query.setStart(start);
        query.setRows(rows);
        if (minExactCount != null) {
            query.set(CommonParams.MIN_EXACT_COUNT, minExactCount);
        }
        return new SolrResultIterator<>(client, limit, maxRows, "collection", query,
            doc -> doc.getFieldValue("id").toString());
    }

    private static List<String> all(SolrResultIterator<String> results) {
        final List<String> read = new ArrayList<>();
        results.forEachRemaining(read::add);
        return read;
    }

    // The offsets and rows of the requests, in pairs
    private void assertPages(int... startsAndRows) {
        assertEquals(IntStream.range(0, startsAndRows.length / 2)
            .mapToObj(i -> Arrays.asList(startsAndRows[2 * i], startsAndRows[2 * i + 1])).collect(Collectors.toList()),
            pages);
    }

    // The last page asks for no more results than those which the first page found leave
    @Test
    public void pagesDoubleUpToTheLargestPage() throws Exception {
        index(100);
        assertEquals(ids, all(read(null, 0, 3, 10)));
        assertPages(0, 3, 3, 6, 9, 10, 19, 10, 29, 10, 39, 10, 49, 10, 59, 10, 69, 10, 79, 10, 89, 10, 99, 1);
    }

    // The last page asks for no more results than the limit leaves, and a read from an offset reads its pages after it
    @Test
    public void pagesStopAtTheLimit() throws Exception {
        index(100);
        assertEquals(ids.subList(5, 25), all(read(20, 5, 3, 10)));
        assertPages(5, 3, 8, 6, 14, 10, 24, 1);
    }

    // A largest page no larger than the query's rows reads every page in pages of the query's rows, as before
    @Test
    public void pagesKeepTheQueryRowsWhereTheLargestPageIsNoLarger() throws Exception {
        index(10);
        assertEquals(ids, all(read(null, 0, 3, 2)));
        assertPages(0, 3, 3, 3, 6, 3, 9, 1);
    }

    // A read ends with the results which its first page found, also from an offset, its last page asking for no more,
    // so that it takes no further request
    @Test
    public void aReadEndsWithTheResultsWhichItsFirstPageFound() throws Exception {
        index(3);
        assertEquals(ids, all(read(null, 0, 3, 10)));
        assertPages(0, 3);
        pages.clear();
        index(10);
        assertEquals(ids, all(read(null, 0, 3, 10)));
        assertPages(0, 3, 3, 6, 9, 1);
        pages.clear();
        index(9);
        assertEquals(ids, all(read(null, 0, 3, 10)));
        assertPages(0, 3, 3, 6);
        pages.clear();
        index(20);
        assertEquals(ids.subList(17, 20), all(read(null, 17, 3, 10)));
        assertPages(17, 3);
    }

    // A page which comes back short isn't the last one where its count says that more remain, as where a distributed
    // request left out a result which was deleted between its phases: the read goes on after it, from the offset of the
    // results read, as the deletion moved each result after the deleted one a place forward
    @Test
    public void aPageWhichComesBackShortWhileMoreRemainIsNotTheLast() {
        index(20);
        lostBetweenPhases.add("d5");
        final List<String> expected = new ArrayList<>(ids);
        expected.remove("d5");
        assertEquals(expected, assertTimeoutPreemptively(Duration.ofSeconds(10), () -> all(read(null, 0, 3, 10))));
        assertPages(0, 3, 3, 6, 8, 10, 18, 2);
    }

    // A page which comes back empty ends the read, whatever its count says, so that every read ends: here every result
    // of the second page is lost between the phases of its request, which ends the read early. Asking whether there is
    // more once the read ended sends no further request
    @Test
    public void aPageWhichComesBackEmptyEndsTheRead() {
        index(20);
        lostBetweenPhases.addAll(ids.subList(3, 9));
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final SolrResultIterator<String> results = read(null, 0, 3, 10);
            assertEquals(Arrays.asList("d0", "d1", "d2"), all(results));
            assertFalse(results.hasNext());
        });
        assertPages(0, 3, 3, 6);
    }

    // A page which asks for more results than the limit, as the rows which a parameter of a direct index query gives,
    // returns the limit of them
    @Test
    public void aPageLargerThanTheLimitReturnsTheLimit() throws Exception {
        index(20);
        assertEquals(ids.subList(0, 5), all(read(5, 0, 10, 10)));
        assertPages(0, 10);
    }

    // A read which finds fewer results than its first page found, as where other transactions deleted some meanwhile,
    // ends with the page after which Solr found no more, where it waited forever for the rest. The deletion of a result
    // read before moves the others forward, so that the next page, at the offset of the results read, skips d3
    @Test
    public void aReadWhichFindsFewerResultsThanItsFirstPageEnds() {
        index(10);
        final List<String> read = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final SolrResultIterator<String> results = read(null, 0, 3, 3);
            final List<String> taken = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                results.hasNext();
                taken.add(results.next());
            }
            ids.remove("d0");
            ids.remove("d9");
            results.forEachRemaining(taken::add);
            return taken;
        });
        assertEquals(Arrays.asList("d0", "d1", "d2", "d4", "d5", "d6", "d7", "d8"), read);
        assertPages(0, 3, 3, 3, 6, 3);
    }

    // A page is the last one where Solr found no more results than those up to its end, as its count says, also from
    // an offset, as where other transactions deleted results since the first page: here d18 and d19, which the third
    // page would have read, so that it comes back short and ends the read with no further request
    @Test
    public void thePageAfterWhichSolrFoundNoMoreIsTheLast() {
        index(20);
        final List<String> read = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final SolrResultIterator<String> results = read(null, 5, 3, 10);
            final List<String> taken = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                results.hasNext();
                taken.add(results.next());
            }
            ids.remove("d18");
            ids.remove("d19");
            results.forEachRemaining(taken::add);
            return taken;
        });
        assertEquals(ids.subList(5, 18), read);
        assertPages(5, 3, 8, 6, 14, 6);
    }

    // Results which other transactions add meanwhile can't keep a read going, as it ends with the results which its
    // first page found: here a result is added at the end of 20 for every one which the read takes, so that every page
    // comes back full and Solr finds more than those up to its end. The read keeps only the first results it takes,
    // so that one which never ends fails by the timeout before it fills the heap
    @Test
    public void aReadWhichFindsResultsAddedMeanwhileEnds() {
        final AtomicInteger taken = new AtomicInteger();
        ids = new AbstractList<String>() {
            @Override
            public String get(int index) {
                return "d" + index;
            }

            @Override
            public int size() {
                return 20 + taken.get();
            }
        };
        final List<String> read = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final SolrResultIterator<String> results = read(null, 0, 3, 10);
            final List<String> first = new ArrayList<>();
            while (results.hasNext()) {
                final String id = results.next();
                if (first.size() < 30) {
                    first.add(id);
                }
                taken.incrementAndGet();
            }
            return first;
        });
        assertEquals(IntStream.range(0, 20).mapToObj(i -> "d" + i).collect(Collectors.toList()), read);
        assertPages(0, 3, 3, 6, 9, 10, 19, 1);
    }

    // The count of a request which sets minExactCount, as a direct index query may, can be a lower bound, here just
    // past the end of each page but the last, which bounds no read: the read goes on until a page's count shows no more
    @Test
    public void aCountWhichMayBeALowerBoundBoundsNoRead() {
        index(20);
        minExactCount = 1;
        assertEquals(ids, assertTimeoutPreemptively(Duration.ofSeconds(10), () -> all(read(null, 0, 3, 10))));
        assertPages(0, 3, 3, 6, 9, 10, 19, 10);
    }
}
