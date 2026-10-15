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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The pages of a search of a point in time: each asked for after the last hit of the one before, with the id the
 * last response gave, until a short page, the limit or the consumer's close, which closes the point in time once.
 */
public class ElasticSearchPointInTimeTest {

    private static final int PAGE_SIZE = 3;

    private ElasticSearchClient client;
    private Map<String, Object> requestBody;

    @BeforeEach
    public void setUp() {
        client = mock(ElasticSearchClient.class);
        requestBody = new HashMap<>();
        requestBody.put("size", PAGE_SIZE);
    }

    //A page of hits whose sort values are their positions, with the id of the point in time the next page has to use
    private static ElasticSearchResponse page(int firstPosition, int size, String nextPitId) {
        final List<RawQuery.Result<String>> hits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            hits.add(new RawQuery.Result<>("doc" + (firstPosition + i), 1f));
        }
        final ElasticSearchResponse response = new ElasticSearchResponse();
        response.setResults(hits);
        response.setPitId(nextPitId);
        response.setLastSort(size == 0 ? null : Arrays.asList(1f, firstPosition + size - 1));
        return response;
    }

    private static List<String> drain(ElasticSearchPointInTime pages) {
        final List<String> ids = new ArrayList<>();
        while (pages.hasNext()) {
            ids.add(pages.next().getResult());
        }
        return ids;
    }

    @Test
    public void shouldCloseThePointInTimeAfterAShortFirstPage() throws IOException {
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 2, "pit-1"), PAGE_SIZE, Long.MAX_VALUE);
        verify(client).closePointInTime("pit-1");
        assertEquals(Arrays.asList("doc0", "doc1"), drain(pages));
        verify(client, never()).searchPointInTime(any(), any());
        assertThrows(NoSuchElementException.class, pages::next);
    }

    @Test
    public void shouldAskForEachPageAfterTheLastHitOfTheOneBeforeWithTheLatestId() throws IOException {
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(page(3, 3, "pit-2"));
        when(client.searchPointInTime(eq("pit-2"), any())).thenReturn(page(6, 1, "pit-2"));
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 3, null), PAGE_SIZE, Long.MAX_VALUE);
        verify(client, never()).closePointInTime(any());

        assertEquals(Arrays.asList("doc0", "doc1", "doc2", "doc3", "doc4", "doc5", "doc6"), drain(pages));

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> bodies = ArgumentCaptor.forClass(Map.class);
        verify(client).searchPointInTime(eq("pit-1"), bodies.capture());
        //The body is shared, so the position captured is the latest one; the second page was asked for after doc5
        assertEquals(Arrays.asList(1f, 5), bodies.getValue().get("search_after"));
        verify(client).searchPointInTime(eq("pit-2"), any());
        //The point in time was closed once, under the id the last response gave, after the short page
        verify(client).closePointInTime("pit-2");
        verify(client, times(1)).closePointInTime(any());
    }

    @Test
    public void shouldStopAtTheLimitAndCloseThePointInTime() throws IOException {
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(page(3, 3, "pit-1"));
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 3, "pit-1"), PAGE_SIZE, 5);

        assertEquals(Arrays.asList("doc0", "doc1", "doc2", "doc3", "doc4"), drain(pages));
        //One page beyond the first held the limit; nothing beyond it was asked for
        verify(client, times(1)).searchPointInTime(any(), any());
        verify(client, times(1)).closePointInTime("pit-1");
    }

    @Test
    public void shouldClosePointInTimeOnceWhenTheConsumerStopsBeforeTheEnd() throws IOException {
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 3, "pit-1"), PAGE_SIZE, Long.MAX_VALUE);
        assertEquals("doc0", pages.next().getResult());

        pages.close();
        pages.close();

        assertFalse(pages.hasNext());
        verify(client, times(1)).closePointInTime("pit-1");
        verify(client, never()).searchPointInTime(any(), any());
    }

    @Test
    public void shouldNotFailWhenClosingThePointInTimeFails() throws IOException {
        doThrow(new IOException("gone")).when(client).closePointInTime(any());
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 1, "pit-1"), PAGE_SIZE, Long.MAX_VALUE);
        assertTrue(pages.hasNext());
        assertEquals("doc0", pages.next().getResult());
        assertFalse(pages.hasNext());
    }

    //A page which fails, with an I/O error or at runtime, ends the pages, and the point in time with them
    @Test
    public void shouldCloseThePointInTimeWhenALaterPageFails() throws IOException {
        when(client.searchPointInTime(eq("pit-1"), any())).thenThrow(new IOException("connection reset"));
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 3, "pit-1"), PAGE_SIZE, Long.MAX_VALUE);
        drainFirstPage(pages);
        assertThrows(UncheckedIOException.class, pages::hasNext);
        verify(client, times(1)).closePointInTime("pit-1");
        assertFalse(pages.hasNext());
    }

    @Test
    public void shouldCloseThePointInTimeWhenALaterPageFailsAtRuntime() throws IOException {
        final IllegalStateException interrupted = new IllegalStateException("interrupted while waiting to retry");
        when(client.searchPointInTime(eq("pit-1"), any())).thenThrow(interrupted);
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", requestBody,
            page(0, 3, "pit-1"), PAGE_SIZE, Long.MAX_VALUE);
        drainFirstPage(pages);
        assertSame(interrupted, assertThrows(IllegalStateException.class, pages::hasNext));
        verify(client, times(1)).closePointInTime("pit-1");
        assertFalse(pages.hasNext());
    }

    private static void drainFirstPage(ElasticSearchPointInTime pages) {
        for (int i = 0; i < PAGE_SIZE; i++) {
            pages.next();
        }
    }
}
