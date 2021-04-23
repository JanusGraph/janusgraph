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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ElasticSearchSearchAfterTest {

    @Test
    public void shouldDeletePitIfFinishedOnInitialResponse() throws IOException {
        int batchSize = 5;
        String pitId = "testPitId";

        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);

        Map<String, Object> request = makeTestRequest();
        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize - 1);
        setupResultMocks(initialResults, initialResponse, pitId);

        ElasticSearchSearchAfter it = new ElasticSearchSearchAfter(client, pitId, initialResponse, request, batchSize);
        for (int i = 0; i < batchSize - 1; ++i) {
            it.next();
        }
        Mockito.verify(client).deletePit(pitId);
    }

    @Test
    public void shouldNotDeletePitIfNotFinishedOnInitialResponse() throws IOException {
        int batchSize = 5;
        String pitId = "testPitId";

        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);

        Map<String, Object> request = makeTestRequest();
        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize);
        setupResultMocks(initialResults, initialResponse, pitId);

        ElasticSearchSearchAfter it = new ElasticSearchSearchAfter(client, pitId, initialResponse, request, batchSize);
        for (int i = 0; i < batchSize; ++i) {
            it.next();
        }
        Mockito.verify(client, Mockito.never()).deletePit(pitId);
    }

    @Test
    public void shouldDeletePitIfFinishedOnSecondResponse() throws IOException {
        int batchSize = 5;
        String pitId = "testPitId";

        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize);
        setupResultMocks(initialResults, initialResponse, pitId);

        ElasticSearchResponse secondResponse = Mockito.mock(ElasticSearchResponse.class);
        List<RawQuery.Result<String>> secondResults = makeTestResults(batchSize-1);
        setupResultMocks(secondResults, secondResponse, pitId);

        Map<String, Object> request = makeTestRequest();

        Mockito.when(client.searchAfterWithPit(pitId, request, null)).thenReturn(secondResponse);

        ElasticSearchSearchAfter it = new ElasticSearchSearchAfter(client, pitId, initialResponse, request, batchSize);
        for (int i = 0; i < batchSize; ++i) {
            it.next();
        }
        Mockito.verify(client, Mockito.never()).deletePit(pitId);

        // Move to second page
        it.next();
        for (int i = 0; i < batchSize - 2; ++i) {
            it.next();
        }
        Mockito.verify(client).deletePit(pitId);
    }

    @Test
    public void shouldUseSortValuesOnSecondRequest() throws IOException {
        int batchSize = 5;
        String pitId = "testPitId";
        List<Object> searchAfter = Collections.singletonList(123L);

        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);
        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize, searchAfter);
        setupResultMocks(initialResults, initialResponse, pitId);

        ElasticSearchResponse secondResponse = Mockito.mock(ElasticSearchResponse.class);
        List<RawQuery.Result<String>> secondResults = makeTestResults(batchSize-1, null);
        setupResultMocks(secondResults, secondResponse, pitId);

        Map<String, Object> request = makeTestRequest();

        Mockito.when(client.searchAfterWithPit(pitId, request, searchAfter)).thenReturn(secondResponse);

        ElasticSearchSearchAfter it = new ElasticSearchSearchAfter(client, pitId, initialResponse, request, batchSize);
        for (int i = 0; i < batchSize; ++i) {
            it.next();
        }
        it.next();
        Mockito.verify(client, Mockito.times(1)).searchAfterWithPit(pitId, request, searchAfter);
    }

    @Test
    public void shouldThrowNoSuchElementWhenReadingPastEnd() {
        int batchSize = 5;
        String pitId = "testPitId";

        ElasticSearchClient client = Mockito.mock(ElasticSearchClient.class);
        ElasticSearchResponse initialResponse = Mockito.mock(ElasticSearchResponse.class);

        Map<String, Object> request = makeTestRequest();
        List<RawQuery.Result<String>> initialResults = makeTestResults(batchSize - 1);
        setupResultMocks(initialResults, initialResponse, pitId);

        ElasticSearchSearchAfter it = new ElasticSearchSearchAfter(client, pitId, initialResponse, request, batchSize);
        for (int i = 0; i < batchSize - 1; ++i) {
            it.next();
        }

        Assertions.assertThrows(NoSuchElementException.class, it::next);
    }

    private Map<String, Object> makeTestRequest() {
        return new HashMap<>();
    }

    private List<RawQuery.Result<String>> makeTestResults(int batchSize) {
        return makeTestResults(batchSize, null);
    }

    private List<RawQuery.Result<String>> makeTestResults(int batchSize, List<Object> sortValues) {
        List<RawQuery.Result<String>> initialResults = new LinkedList<>();
        for (int i = 0; i < batchSize; i++) {
            RawQuery.Result<String> result = new RawQuery.Result<>("testResult" + i, 0.9f, (i == batchSize - 1) ? sortValues : null);
            initialResults.add(result);
        }
        return initialResults;
    }

    private void setupResultMocks(List<RawQuery.Result<String>> results, ElasticSearchResponse response, String pitId) {
        Mockito.when(response.getPitId()).thenReturn(pitId);
        Mockito.when(response.getResults()).thenReturn(results.stream());
        Mockito.when(response.numResults()).thenReturn(results.size());
    }
}
