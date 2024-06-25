// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.diskstorage.es.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RestClientBulkRequestsTest {
    @Mock
    private RestClient restClientMock;

    @Mock
    private Response response;

    @Mock
    private StatusLine statusLine;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    RestElasticSearchClient createClient(int bulkChunkSerializedLimitBytes) throws IOException {
        //Just throw an exception when there's an attempt to look up the ES version during instantiation
        when(restClientMock.performRequest(any())).thenThrow(new IOException());

        RestElasticSearchClient clientUnderTest = new RestElasticSearchClient(restClientMock, 0, false,
            0, Collections.emptySet(), 0, 0, bulkChunkSerializedLimitBytes);
        //There's an initial query to get the ES version we need to accommodate, and then reset for the actual test
        Mockito.reset(restClientMock);
        return clientUnderTest;
    }

    @Test
    public void testSplittingOfLargeBulkItems() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        when(statusLine.getStatusCode()).thenReturn(200);

        //In both cases return a "success"
        RestBulkResponse singletonBulkItemResponseSuccess = new RestBulkResponse();
        singletonBulkItemResponseSuccess.setItems(
            Collections.singletonList(Collections.singletonMap("index", new RestBulkResponse.RestBulkItemResponse())));
        byte [] singletonBulkItemResponseSuccessBytes = mapper.writeValueAsBytes(singletonBulkItemResponseSuccess);
        HttpEntity singletonBulkItemHttpEntityMock = mock(HttpEntity.class);
        when(singletonBulkItemHttpEntityMock.getContent())
            .thenReturn(new ByteArrayInputStream(singletonBulkItemResponseSuccessBytes))
            //Have to setup a second input stream because it will have been consumed by the first pass
            .thenReturn(new ByteArrayInputStream(singletonBulkItemResponseSuccessBytes));
        when(response.getEntity()).thenReturn(singletonBulkItemHttpEntityMock);
        when(response.getStatusLine()).thenReturn(statusLine);

        int bulkLimit = 800;
        try (RestElasticSearchClient restClientUnderTest = createClient(bulkLimit)) {
            //prime the restClientMock again after it's reset after creation
            when(restClientMock.performRequest(any())).thenReturn(response).thenReturn(response);
            StringBuilder payloadBuilder = new StringBuilder();
            IntStream.range(0, bulkLimit - 100).forEach(value -> payloadBuilder.append("a"));
            String largePayload = payloadBuilder.toString();
            restClientUnderTest.bulkRequest(Arrays.asList(
                //There should be enough characters in the payload that they can't both be sent in a single bulk call
                ElasticSearchMutation.createIndexRequest("some_index", "some_type", "some_doc_id1",
                    Collections.singletonMap("someKey", largePayload)),
                ElasticSearchMutation.createIndexRequest("some_index", "some_type", "some_doc_id2",
                    Collections.singletonMap("someKey", largePayload))
            ), null);
            //Verify that despite only calling bulkRequest() once, we had 2 calls to the underlying rest client's
            //perform request (due to the mutations being split across 2 calls)
            verify(restClientMock, times(2)).performRequest(requestCaptor.capture());
        }
    }

    @Test
    public void testThrowingForOverlyLargeBulkItemOnlyAfterSmallerItemsAreChunked() throws IOException {
        int bulkLimit = 1_000_000;
        StringBuilder overlyLargePayloadBuilder = new StringBuilder();
        IntStream.range(0, bulkLimit * 10).forEach(value -> overlyLargePayloadBuilder.append("a"));
        String overlyLargePayload = overlyLargePayloadBuilder.toString();
        ElasticSearchMutation overlyLargeMutation = ElasticSearchMutation.createIndexRequest("some_index", "some_type", "some_doc_id2",
            Collections.singletonMap("someKey", overlyLargePayload));
        List<ElasticSearchMutation> bulkItems = Arrays.asList(
            ElasticSearchMutation.createIndexRequest("some_index", "some_type", "some_doc_id1",
                Collections.singletonMap("someKey", "small_payload1")),
            overlyLargeMutation,
            ElasticSearchMutation.createIndexRequest("some_index", "some_type", "some_doc_id3",
                Collections.singletonMap("someKey", "small_payload2"))
        );

        try (RestElasticSearchClient restClientUnderTest = createClient(bulkLimit)) {
            RestElasticSearchClient.BulkRequestChunker chunkerUnderTest = restClientUnderTest.new BulkRequestChunker(bulkItems);
            int overlyLargeRequestExpectedSize = restClientUnderTest.new RequestBytes(overlyLargeMutation).getSerializedSize();

            //The chunker should chunk this request first as a list of the 2 smaller items
            List<RestElasticSearchClient.RequestBytes> smallItemsChunk = chunkerUnderTest.next();
            Assertions.assertEquals(2, smallItemsChunk.size());

            //Then the chunker should still return true for hasNext()
            Assertions.assertTrue(chunkerUnderTest.hasNext());

            //Then the next call for next() should throw to report the exceptionally large item
            IllegalArgumentException thrownException = Assertions.assertThrows(IllegalArgumentException.class, chunkerUnderTest::next,
                "Should have thrown due to bulk request item being too large");

            String expectedExceptionMessage = String.format("Bulk request item(s) larger than permitted chunk limit. Limit is %s. Serialized item size(s) [%s]",
                bulkLimit, overlyLargeRequestExpectedSize);

            Assertions.assertEquals(expectedExceptionMessage, thrownException.getMessage());
        }
    }

    @Test
    public void testThrowingIfSingleBulkItemIsLargerThanLimit() throws IOException {
        int bulkLimit = 800;
        try (RestElasticSearchClient restClientUnderTest = createClient(bulkLimit)) {
            StringBuilder payloadBuilder = new StringBuilder();
            //This payload is too large to send given the set limit, since it is a single item we can't split it
            IntStream.range(0, bulkLimit * 10).forEach(value -> payloadBuilder.append("a"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> restClientUnderTest.bulkRequest(
                Collections.singletonList(
                    ElasticSearchMutation.createIndexRequest("some_index", "some_type", "some_doc_id",
                        Collections.singletonMap("someKey", payloadBuilder.toString()))
            ), null), "Should have thrown due to bulk request item being too large");
        }
    }
}
