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
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

    //The action line of an update carries retry_on_conflict above 0, which is Elasticsearch's own default
    @Test
    public void testRetryOnConflictIsSentWithEveryUpdateAboveZero() throws IOException {
        final ElasticSearchMutation update = ElasticSearchMutation.createUpdateRequest("some_index", "some_type",
            "some_doc_id", ImmutableMap.builder().put("doc", ImmutableMap.of("name", "value")), null);
        final ElasticSearchMutation index = ElasticSearchMutation.createIndexRequest("some_index", "some_type",
            "some_doc_id", ImmutableMap.of("name", "value"));
        try (RestElasticSearchClient restClientUnderTest = createClient(100_000_000)) {
            restClientUnderTest.setRetryOnConflict(3);
            Assertions.assertEquals(3, action(restClientUnderTest.new RequestBytes(update), "update").get("retry_on_conflict"));
            Assertions.assertFalse(action(restClientUnderTest.new RequestBytes(index), "index").containsKey("retry_on_conflict"));

            restClientUnderTest.setRetryOnConflict(0);
            Assertions.assertFalse(action(restClientUnderTest.new RequestBytes(update), "update").containsKey("retry_on_conflict"));
        }
        //Elasticsearch 6 reads the same key and only deprecates its older name, _retry_on_conflict
        try (RestElasticSearchClient elasticsearch6 = new RestElasticSearchClient(restClientMock, 0, false, 0,
            Collections.emptySet(), 0, 0, 100_000_000, ElasticMajorVersion.SIX)) {
            elasticsearch6.setRetryOnConflict(3);
            final Map<String, Object> updateAction = action(elasticsearch6.new RequestBytes(update), "update");
            Assertions.assertEquals(3, updateAction.get("retry_on_conflict"));
            Assertions.assertFalse(updateAction.containsKey("_retry_on_conflict"));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> action(RestElasticSearchClient.RequestBytes request, String type) throws IOException {
        final Map<String, Object> actionLine = new ObjectMapper().readValue(request.requestBytes, Map.class);
        Assertions.assertEquals(Collections.singleton(type), actionLine.keySet());
        return (Map<String, Object>) actionLine.get(type);
    }

    //The action line names the operation, the index, the mapping type up to Elasticsearch 6 and the id, whatever
    //characters they hold, and whatever the JVM's default locale makes of the operation's name: Turkish lower cases the
    //I of INDEX to a dotless i
    @Test
    public void testTheActionLineOfEachOperation() throws IOException {
        final String id = "quote\" backslash\\ bullet\u2022";
        final Locale defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        try (RestElasticSearchClient restClientUnderTest = createClient(100_000_000)) {
            restClientUnderTest.setRetryOnConflict(3);
            Assertions.assertEquals(ImmutableMap.of("index", ImmutableMap.of("_index", "some_index", "_id", id)),
                actionLine(restClientUnderTest.new RequestBytes(ElasticSearchMutation.createIndexRequest("some_index",
                    "some_type", id, ImmutableMap.of("name", "value")))));
            Assertions.assertEquals(ImmutableMap.of("update",
                    ImmutableMap.of("_index", "some_index", "_id", id, "retry_on_conflict", 3)),
                actionLine(restClientUnderTest.new RequestBytes(ElasticSearchMutation.createUpdateRequest("some_index",
                    "some_type", id, ImmutableMap.of("doc", ImmutableMap.of("name", "value"))))));
            Assertions.assertEquals(ImmutableMap.of("delete", ImmutableMap.of("_index", "some_index", "_id", id)),
                actionLine(restClientUnderTest.new RequestBytes(ElasticSearchMutation.createDeleteRequest("some_index",
                    "some_type", id))));
        } finally {
            Locale.setDefault(defaultLocale);
        }
        try (RestElasticSearchClient elasticsearch6 = new RestElasticSearchClient(restClientMock, 0, false, 0,
            Collections.emptySet(), 0, 0, 100_000_000, ElasticMajorVersion.SIX)) {
            Assertions.assertEquals(ImmutableMap.of("delete",
                    ImmutableMap.of("_index", "some_index", "_type", "some_type", "_id", "some_doc_id")),
                actionLine(elasticsearch6.new RequestBytes(ElasticSearchMutation.createDeleteRequest("some_index",
                    "some_type", "some_doc_id"))));
        }
    }

    private static Map<?, ?> actionLine(RestElasticSearchClient.RequestBytes request) throws IOException {
        return new ObjectMapper().readValue(request.requestBytes, Map.class);
    }

    //An item of every shape: with a source (an index request and an update's script), without one (a deletion), and
    //values outside ASCII, whose bytes are more than their characters
    private static List<RestElasticSearchClient.RequestBytes> requestsOfEveryShape(RestElasticSearchClient client)
        throws IOException {
        return Arrays.asList(
            client.new RequestBytes(ElasticSearchMutation.createIndexRequest("some_index", "some_type", "doc1",
                ImmutableMap.of("name", "\u00fcml\u00e4ut", "count", 3))),
            client.new RequestBytes(ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "doc2")),
            client.new RequestBytes(ElasticSearchMutation.createUpdateRequest("some_index", "some_type", "doc3",
                ImmutableMap.of("script", ImmutableMap.of("id", "some_script", "params", ImmutableMap.of("v", 1))))),
            client.new RequestBytes(ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "doc4")));
    }

    //What the ByteArrayOutputStream which the entity replaces put together: each item's action line and a new line,
    //then its source and another new line if it has one
    private static byte[] concatenated(List<RestElasticSearchClient.RequestBytes> requests) throws IOException {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (final RestElasticSearchClient.RequestBytes request : requests) {
            body.write(request.requestBytes);
            body.write('\n');
            if (request.requestSource != null) {
                body.write(request.requestSource);
                body.write('\n');
            }
        }
        return body.toByteArray();
    }

    @Test
    public void testTheBodyIsTheItemsBytesInTheirOrder() throws IOException {
        try (RestElasticSearchClient restClientUnderTest = createClient(100_000_000)) {
            final List<RestElasticSearchClient.RequestBytes> requests = requestsOfEveryShape(restClientUnderTest);
            final byte[] expected = concatenated(requests);
            final RestElasticSearchClient.BulkRequestEntity entity = new RestElasticSearchClient.BulkRequestEntity(requests);

            Assertions.assertEquals(expected.length, entity.getContentLength());
            Assertions.assertEquals(ContentType.APPLICATION_JSON.toString(), entity.getContentType().getValue());
            Assertions.assertTrue(entity.isRepeatable());
            Assertions.assertFalse(entity.isStreaming());
            final ByteArrayOutputStream written = new ByteArrayOutputStream();
            entity.writeTo(written);
            Assertions.assertArrayEquals(expected, written.toByteArray());
            //Read twice, as a reattempt against another node would
            Assertions.assertArrayEquals(expected, EntityUtils.toByteArray(entity));
            Assertions.assertArrayEquals(expected, EntityUtils.toByteArray(entity));

            final RestElasticSearchClient.BulkRequestEntity empty = new RestElasticSearchClient.BulkRequestEntity(
                Collections.emptyList());
            Assertions.assertEquals(0, empty.getContentLength());
            Assertions.assertEquals(-1, empty.getContent().read());
        }
    }

    //The HTTP client reads the body through a channel, which stops at the first read after which nothing is
    //available, so a read fills what it can across items and available() counts what is left
    @Test
    public void testTheBodyIsReadAcrossItemsInBuffersOfAnySize() throws IOException {
        try (RestElasticSearchClient restClientUnderTest = createClient(100_000_000)) {
            final byte[] expected = concatenated(requestsOfEveryShape(restClientUnderTest));
            final RestElasticSearchClient.BulkRequestEntity entity =
                new RestElasticSearchClient.BulkRequestEntity(requestsOfEveryShape(restClientUnderTest));
            for (final int bufferSize : new int[] {1, 2, 3, 7, 64, expected.length, expected.length + 10}) {
                final ByteArrayOutputStream read = new ByteArrayOutputStream();
                try (InputStream content = entity.getContent()) {
                    Assertions.assertEquals(expected.length, content.available());
                    //The bytes go behind an offset, to see that reads respect it
                    final byte[] buffer = new byte[bufferSize + 2];
                    int count;
                    while ((count = content.read(buffer, 2, bufferSize)) != -1) {
                        //Every read fills the buffer, whatever the items' boundaries, but the last
                        Assertions.assertTrue(count == bufferSize || read.size() + count == expected.length,
                            "a read of " + count + " into " + bufferSize);
                        read.write(buffer, 2, count);
                        Assertions.assertEquals(expected.length - read.size(), content.available());
                    }
                    Assertions.assertEquals(-1, content.read(buffer, 2, bufferSize));
                    Assertions.assertEquals(-1, content.read());
                    Assertions.assertEquals(0, content.read(buffer, 0, 0));
                }
                Assertions.assertArrayEquals(expected, read.toByteArray(), "buffers of " + bufferSize);
            }
            final ByteArrayOutputStream byteByByte = new ByteArrayOutputStream();
            try (InputStream content = entity.getContent()) {
                int b;
                while ((b = content.read()) != -1) {
                    byteByByte.write(b);
                    Assertions.assertEquals(expected.length - byteByByte.size(), content.available());
                }
            }
            Assertions.assertArrayEquals(expected, byteByByte.toByteArray());
        }
    }

    //Request signers, such as those of the AWS SDK, mark the body, read it whole to hash it and reset it before it is
    //sent, which a ByteArrayInputStream over the body supported
    @Test
    public void testTheBodyCanBeReadAgainFromAMark() throws IOException {
        try (RestElasticSearchClient restClientUnderTest = createClient(100_000_000)) {
            final byte[] expected = concatenated(requestsOfEveryShape(restClientUnderTest));
            final RestElasticSearchClient.BulkRequestEntity entity =
                new RestElasticSearchClient.BulkRequestEntity(requestsOfEveryShape(restClientUnderTest));
            try (InputStream content = entity.getContent()) {
                Assertions.assertTrue(content.markSupported());
                //Without a mark, reset goes back to the start
                Assertions.assertArrayEquals(expected, content.readAllBytes());
                content.reset();
                Assertions.assertEquals(expected.length, content.available());
                Assertions.assertArrayEquals(expected, content.readAllBytes());
            }
            //From every position: in the middle of a part, at the end of one, and at the end of the body
            for (int marked = 0; marked <= expected.length; marked++) {
                final byte[] rest = Arrays.copyOfRange(expected, marked, expected.length);
                try (InputStream content = entity.getContent()) {
                    Assertions.assertArrayEquals(Arrays.copyOfRange(expected, 0, marked), content.readNBytes(marked));
                    content.mark(0);
                    Assertions.assertArrayEquals(rest, content.readAllBytes(), "read from " + marked);
                    Assertions.assertEquals(0, content.available());
                    content.reset();
                    Assertions.assertEquals(rest.length, content.available(), "available again from " + marked);
                    Assertions.assertArrayEquals(rest, content.readAllBytes(), "read again from " + marked);
                }
            }
        }
    }

    //A bulk request sends its items' bytes, and asks for the fields of the response which the client reads
    @Test
    public void testABulkRequestAsksForTheFieldsOfTheResponseWhichItReads() throws IOException {
        when(statusLine.getStatusCode()).thenReturn(200);
        final HttpEntity responseEntity = mock(HttpEntity.class);
        when(responseEntity.getContent()).thenReturn(new ByteArrayInputStream(
            "{\"errors\":false,\"items\":[{\"index\":{\"status\":201}}]}".getBytes(StandardCharsets.UTF_8)));
        when(response.getEntity()).thenReturn(responseEntity);
        when(response.getStatusLine()).thenReturn(statusLine);
        final ElasticSearchMutation mutation = ElasticSearchMutation.createIndexRequest("some_index", "some_type",
            "some_doc_id", Collections.singletonMap("someKey", "value"));
        try (RestElasticSearchClient restClientUnderTest = createClient(100_000_000)) {
            when(restClientMock.performRequest(any())).thenReturn(response);
            restClientUnderTest.bulkRequest(Collections.singletonList(mutation), "some_pipeline");

            verify(restClientMock).performRequest(requestCaptor.capture());
            final Request request = requestCaptor.getValue();
            Assertions.assertEquals("POST", request.getMethod());
            Assertions.assertEquals("/_bulk?pipeline=some_pipeline", request.getEndpoint());
            Assertions.assertEquals(Collections.singletonMap("filter_path", "errors,items.index.status,items.index.error,"
                    + "items.update.status,items.update.error,items.delete.status,items.delete.error"),
                request.getParameters());
            Assertions.assertArrayEquals(
                concatenated(Collections.singletonList(restClientUnderTest.new RequestBytes(mutation))),
                EntityUtils.toByteArray(request.getEntity()));
        }
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
    public void testLeavingOutACompleteDocumentWhichMakesAnUpdateTooLarge() throws IOException {
        final int bulkLimit = 1000;
        final String largeValue = String.join("", Collections.nCopies(2 * bulkLimit, "a"));
        final ElasticSearchMutation fits = ElasticSearchMutation.createUpdateRequestWithCompleteDocument("some_index",
            "some_type", "fits", ImmutableMap.<String, Object>builder().put("doc", Collections.singletonMap("small", "value")),
            Collections.singletonMap("small", "value"), false);
        final ElasticSearchMutation tooLarge = ElasticSearchMutation.createUpdateRequestWithCompleteDocument(
            "some_index", "some_type", "too_large",
            ImmutableMap.<String, Object>builder().put("doc", Collections.singletonMap("small", "value")),
            Collections.singletonMap("large", largeValue), false);
        try (RestElasticSearchClient restClientUnderTest = createClient(bulkLimit)) {
            final RestElasticSearchClient.BulkRequestChunker chunkerUnderTest =
                restClientUnderTest.new BulkRequestChunker(Arrays.asList(fits, tooLarge));
            final List<RestElasticSearchClient.RequestBytes> chunk = chunkerUnderTest.next();
            //Both are sent, and nothing is left over to fail as too large
            Assertions.assertEquals(2, chunk.size());
            Assertions.assertFalse(chunkerUnderTest.hasNext());
            //The one which fits keeps its complete document, the other goes without it
            Assertions.assertEquals(restClientUnderTest.new RequestBytes(fits).getSerializedSize(),
                chunk.get(0).getSerializedSize());
            Assertions.assertEquals(restClientUnderTest.new RequestBytes(tooLarge.withoutCompleteDocument())
                .getSerializedSize(), chunk.get(1).getSerializedSize());
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
