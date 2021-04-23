// Copyright 2020 JanusGraph Authors
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class ElasticSearchScrollTest {

    @Test
    public void shouldDeleteScrollIfFinishedOnInitialResponse() throws IOException {
        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        int batchSize = 5;
        String scrollId = "testScrollId";


        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize-1);
        setupResultMocks(initialResults, initialResponse, scrollId);

        new ElasticSearchScroll(client, initialResponse, batchSize);

        Mockito.verify(client).deleteScroll(scrollId);
    }

    @Test
    public void shouldNotDeleteScrollIfNotFinishedOnInitialResponse() throws IOException {
        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        int batchSize = 5;
        String scrollId = "testScrollId";

        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize);
        setupResultMocks(initialResults, initialResponse, scrollId);

        new ElasticSearchScroll(client, initialResponse, batchSize);

        Mockito.verify(client, Mockito.never()).deleteScroll(scrollId);
    }

    @Test
    public void shouldDeleteScrollIfFinishedOnSecondResponse() throws IOException {
        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        ElasticSearchResponse secondResponse = Mockito.mock(ElasticSearchResponse.class);
        int batchSize = 5;
        String scrollId = "testScrollId";

        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize);
        setupResultMocks(initialResults, initialResponse, scrollId);

        List<RawQuery.Result<String>> secondResults = makeTestResults(batchSize-1);
        setupResultMocks(secondResults, secondResponse, scrollId);

        Mockito.when(client.search(scrollId)).thenReturn(secondResponse);

        ElasticSearchScroll scroll = new ElasticSearchScroll(client, initialResponse, batchSize);

        for(int i=0; i<batchSize; i++){
            scroll.next();
        }

        Mockito.verify(client, Mockito.never()).deleteScroll(Mockito.any());

        scroll.next();

        Mockito.verify(client).deleteScroll(scrollId);
    }

    @Test
    public void shouldUseTheMostRecentScrollId() throws IOException {
        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        String initialScrollId = "initialScrollId";
        ElasticSearchResponse secondResponse = Mockito.mock(ElasticSearchResponse.class);
        String secondScrollId = "secondScrollId";
        ElasticSearchResponse thirdResponse = Mockito.mock(ElasticSearchResponse.class);
        String thirdScrollId = "thirdScrollId";
        int batchSize = 5;

        // Three pages with batchSize, batchSize and batchSize-1 results and different scrollIds on each page
        setupResultMocks(makeTestResults(batchSize), initialResponse, initialScrollId);
        Mockito.when(client.search(initialScrollId)).thenReturn(secondResponse);
        setupResultMocks(makeTestResults(batchSize), secondResponse, secondScrollId);
        Mockito.when(client.search(secondScrollId)).thenReturn(thirdResponse);
        setupResultMocks(makeTestResults(batchSize - 1), thirdResponse, thirdScrollId);

        // Initialize
        ElasticSearchScroll scroll = new ElasticSearchScroll(client, initialResponse, batchSize);
        for (int i = 0; i < batchSize; i++) {
            scroll.next();
        }

        // Move to second page (new request on next/hasNext)
        scroll.next();
        Mockito.verify(client, Mockito.times(1)).search(initialScrollId);
        for (int i = 0; i < batchSize - 1; i++) {
            scroll.next();
        }

        // Move to third page (new request on next/hasNext)
        scroll.next();
        Mockito.verify(client, Mockito.times(1)).search(secondScrollId);

        // Delete should be called since this is the last page
        Mockito.verify(client).deleteScroll(thirdScrollId);

        for (int i = 0; i < batchSize - 2; i++) {
            scroll.next();
        }

        Assertions.assertThrows(NoSuchElementException.class, scroll::next);
    }

    @Test
    public void shouldThrowNoSuchElementWhenReadingPastEnd() throws IOException {
        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        int batchSize = 5;
        String scrollId = "testScrollId";

        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize-1);
        setupResultMocks(initialResults, initialResponse, scrollId);

        ElasticSearchScroll scroll = new ElasticSearchScroll(client, initialResponse, batchSize);

        for(int i=0; i<batchSize-1; i++){
            scroll.next();
        }

        Assertions.assertThrows(NoSuchElementException.class, scroll::next);
    }

    private List<RawQuery.Result<String>> makeTestResults(int batchSize){
        List<RawQuery.Result<String>> initialResults = new LinkedList<>();
        for(int i=0;i<batchSize;i++){
            RawQuery.Result<String> result = new RawQuery.Result<>("testResult"+i, 0.9);
            initialResults.add(result);
        }
        return initialResults;
    }

    private void setupResultMocks(List<RawQuery.Result<String>> initialResults, ElasticSearchResponse initialResponse, String scrollId) {
        Mockito.when(initialResponse.getScrollId()).thenReturn(scrollId);
        Mockito.when(initialResponse.getResults()).thenReturn(initialResults.stream());
        Mockito.when(initialResponse.numResults()).thenReturn(initialResults.size());
    }
}
