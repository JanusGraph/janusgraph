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

package org.janusgraph.diskstorage.es.rest;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.janusgraph.diskstorage.es.ElasticSearchBulkFailureException;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//A bulk response reports item level failures inside an otherwise successful HTTP response. Every 404 used to be treated
//as a success regardless of the mutation which produced it. That is right for a mutation which only removes content,
//because an absent document already satisfies it, and both a whole document deletion and a script which deletes fields
//qualify. It is wrong for a mutation which adds content: there the 404 is a document_missing_exception and the write did
//not happen. A transaction which removes one field of a document and adds another produces a field deletion and an
//addition against the same document, and mutate() withholds the upsert from the addition, so the addition is the item
//worth reporting.
@ExtendWith(MockitoExtension.class)
public class RestClientBulkItemStatusTest {

    private static final String INDEX = "some_index";
    private static final String TYPE = "some_type";
    private static final String DOCUMENT_MISSING = "document_missing_exception";
    private static final String INDEX_NOT_FOUND = "index_not_found_exception";
    private static final String OTHER_ERROR = "an_error";
    //Distinct per item, so that a test can say which item was reported rather than counting occurrences of a shared
    //string in the message
    private static final String FIELD_DELETION_ERROR = "error_of_the_field_deletion";
    private static final String ADDITION_ERROR = "error_of_the_addition";
    private static final String WHOLE_DELETION_ERROR = "error_of_the_whole_document_deletion";

    @Mock
    private RestClient restClientMock;

    @Mock
    private StatusLine statusLine;

    private RestElasticSearchClient createClient() throws IOException {
        return createClient(100_000_000);
    }

    private RestElasticSearchClient createClient(int bulkChunkSerializedLimitBytes) throws IOException {
        when(restClientMock.performRequest(any())).thenThrow(new IOException());
        final RestElasticSearchClient clientUnderTest = new RestElasticSearchClient(restClientMock, 0, false,
            0, Collections.emptySet(), 0, 0, bulkChunkSerializedLimitBytes);
        Mockito.reset(restClientMock);
        return clientUnderTest;
    }

    //Builds a bulk response in which each submitted item reports the given status, and a non-null error when the
    //status is a failure
    private Response bulkResponseWith(List<String> operations, List<Integer> statuses) throws IOException {
        final List<Object> errors = new ArrayList<>();
        for (final Integer status : statuses) {
            if (status < 300) {
                errors.add(null);
            } else if (status == HttpStatus.SC_NOT_FOUND) {
                errors.add(DOCUMENT_MISSING);
            } else {
                errors.add(OTHER_ERROR);
            }
        }
        return bulkResponseWith(operations, statuses, errors);
    }

    private Response bulkResponseWith(List<String> operations, List<Integer> statuses, List<Object> errors)
        throws IOException {
        //Stated here so that a test set up with mismatched lists fails saying so, rather than through an
        //IndexOutOfBoundsException from the loop below
        assertEquals(operations.size(), statuses.size(), "one status per operation is required");
        assertEquals(operations.size(), errors.size(), "one error per operation is required");
        final RestBulkResponse bulkResponse = new RestBulkResponse();
        final List<Map<String, RestBulkResponse.RestBulkItemResponse>> items = new ArrayList<>();
        for (int i = 0; i < operations.size(); i++) {
            final RestBulkResponse.RestBulkItemResponse item = new RestBulkResponse.RestBulkItemResponse();
            item.setStatus(statuses.get(i));
            item.setError(errors.get(i));
            items.add(Collections.singletonMap(operations.get(i), item));
        }
        bulkResponse.setItems(items);

        final HttpEntity entity = Mockito.mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(new ObjectMapper().writeValueAsBytes(bulkResponse)));
        when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        final Response response = Mockito.mock(Response.class);
        when(response.getEntity()).thenReturn(entity);
        when(response.getStatusLine()).thenReturn(statusLine);
        return response;
    }

    //An update which adds content. Without an upsert Elasticsearch answers 404 when the document is absent
    private static ElasticSearchMutation update(String id) {
        return ElasticSearchMutation.createUpdateRequest(INDEX, TYPE, id,
            ImmutableMap.builder().put("doc", ImmutableMap.of("name", "value")), null);
    }

    //An update which runs the script mutate() uses to take fields out of a document
    private static ElasticSearchMutation fieldDeletion(String id) {
        return ElasticSearchMutation.createFieldDeletionRequest(INDEX, TYPE, id,
            ImmutableMap.of("script", ImmutableMap.of("id", "deletion_script")));
    }

    private void bulkRequest(List<ElasticSearchMutation> mutations, List<String> operations, List<Integer> statuses)
        throws IOException {
        bulkRequest(mutations, operations, statuses, null);
    }

    private void bulkRequest(List<ElasticSearchMutation> mutations, List<String> operations, List<Integer> statuses,
                             List<Object> errors) throws IOException {
        try (RestElasticSearchClient clientUnderTest = createClient()) {
            //Built before the stubbing below, because it stubs mocks of its own
            final Response bulkResponse = errors == null ? bulkResponseWith(operations, statuses)
                : bulkResponseWith(operations, statuses, errors);
            when(restClientMock.performRequest(any())).thenReturn(bulkResponse);
            clientUnderTest.bulkRequest(mutations, null);
        }
    }

    //Elasticsearch describes the reason for a failed item in a map with a type. A removal is exempt from a 404 which
    //says the document is missing, but not from one which says the whole index is missing
    private static Map<String, Object> reason(String type) {
        return ImmutableMap.of("type", type, "reason", type + " for the test");
    }

    @Test
    public void shouldReportAWholeDocumentDeletionAgainstAMissingIndex() throws IOException {
        final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class, () -> bulkRequest(
            Collections.singletonList(ElasticSearchMutation.createDeleteRequest(INDEX, TYPE, "doc1")),
            Collections.singletonList("delete"), Collections.singletonList(HttpStatus.SC_NOT_FOUND),
            Collections.singletonList(reason(INDEX_NOT_FOUND))));
        assertEquals(Collections.singletonList(reason(INDEX_NOT_FOUND)), e.getFailedItems());
    }

    @Test
    public void shouldReportAFieldDeletionAgainstAMissingIndex() throws IOException {
        final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class, () -> bulkRequest(
            Collections.singletonList(fieldDeletion("doc1")), Collections.singletonList("update"),
            Collections.singletonList(HttpStatus.SC_NOT_FOUND), Collections.singletonList(reason(INDEX_NOT_FOUND))));
        assertEquals(Collections.singletonList(reason(INDEX_NOT_FOUND)), e.getFailedItems());
    }

    @Test
    public void shouldTreatTheFieldDeletionOfAnAbsentDocumentAsSuccessWhenTheReasonSaysSo() throws IOException {
        //The reason Elasticsearch actually gives for the field deletion script against an absent document
        bulkRequest(Collections.singletonList(fieldDeletion("doc1")), Collections.singletonList("update"),
            Collections.singletonList(HttpStatus.SC_NOT_FOUND), Collections.singletonList(reason(DOCUMENT_MISSING)));
    }

    @Test
    public void shouldTreatA404CarryingAnotherErrorForAWholeDocumentDeletionAsSuccess() throws IOException {
        //A 404 of a deletion whose reason is anything but a missing index: nothing is thrown, because only a missing
        //index is a condition a removal did not ask for. The no-error variant, which is what Elasticsearch actually
        //answers a deletion of a merely absent document with, is the next test
        bulkRequest(Collections.singletonList(ElasticSearchMutation.createDeleteRequest(INDEX, TYPE, "doc1")),
            Collections.singletonList("delete"), Collections.singletonList(HttpStatus.SC_NOT_FOUND),
            Collections.singletonList(reason(OTHER_ERROR)));
    }

    @Test
    public void shouldIgnoreADeletionWhichElasticsearchReportedWithoutAnError() throws IOException {
        //The shape Elasticsearch actually answers a whole document deletion of an absent document with: status 404,
        //result not_found and no error at all. Such an item never reaches the 404 exemption, because only an item
        //carrying an error is considered for the failure list
        try (RestElasticSearchClient clientUnderTest = createClient()) {
            final Response bulkResponse = bulkResponseWith(Collections.singletonList("delete"),
                Collections.singletonList(HttpStatus.SC_NOT_FOUND), Collections.singletonList(null));
            when(restClientMock.performRequest(any())).thenReturn(bulkResponse);
            clientUnderTest.bulkRequest(
                Collections.singletonList(ElasticSearchMutation.createDeleteRequest(INDEX, TYPE, "doc1")), null);
        }
    }

    @Test
    public void shouldTreatTheFieldDeletionOfAnAbsentDocumentAsSuccess() throws IOException {
        //Nothing is thrown: a document with no fields left to delete is the state the mutation asked for. Elasticsearch
        //reports this as an update of a missing document, the same way it reports an addition which was lost
        bulkRequest(Collections.singletonList(fieldDeletion("doc1")),
            Collections.singletonList("update"), Collections.singletonList(HttpStatus.SC_NOT_FOUND));
    }

    @Test
    public void shouldReportAnUpdateOfAMissingDocument() throws IOException {
        final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class,
            () -> bulkRequest(Collections.singletonList(update("doc1")),
                Collections.singletonList("update"), Collections.singletonList(HttpStatus.SC_NOT_FOUND)));
        //Asserted on the exception's own record of the failed items rather than on its message, which is bounded
        //and may be reworded
        assertEquals(Collections.singletonList(DOCUMENT_MISSING), e.getFailedItems());
        assertEquals(Collections.singleton(HttpStatus.SC_NOT_FOUND), e.getFailedItemStatusCodes());
    }

    @Test
    public void shouldReportAnIndexRequestWhichReturnedNotFound() throws IOException {
        //An index request creates the document, so a 404 is not something it can ask for
        assertThrows(ElasticSearchBulkFailureException.class, () -> bulkRequest(
            Collections.singletonList(ElasticSearchMutation.createIndexRequest(INDEX, TYPE, "doc1",
                ImmutableMap.of("name", "value"))),
            Collections.singletonList("index"), Collections.singletonList(HttpStatus.SC_NOT_FOUND)));
    }

    @Test
    public void shouldReportOnlyTheAdditionWhenAFieldDeletionAccompaniesIt() throws IOException {
        //This is the shape mutate() produces when a transaction removes one field of an absent document and adds
        //another: the field deletion script and the addition against the same document. mutate() leaves the addition
        //without an upsert once the mutation has deletions, so the addition is the half which lost a write and the
        //only half worth reporting. A value change on a SINGLE cardinality key is not this shape, because
        //IndexTransaction consolidates the deletion of the old value away and the addition then carries an upsert;
        //ElasticsearchIndexTest pins both against a live Elasticsearch
        final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class, () -> bulkRequest(
            Arrays.asList(fieldDeletion("doc1"), update("doc1")),
            Arrays.asList("update", "update"), Arrays.asList(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_NOT_FOUND),
            Arrays.asList(FIELD_DELETION_ERROR, ADDITION_ERROR)));
        assertEquals(Collections.singletonList(ADDITION_ERROR), e.getFailedItems());
        assertEquals(Collections.singletonMap(TYPE, Collections.singleton("doc1")), e.getFailedDocumentsByStore());
    }

    @Test
    public void shouldStillReportOtherFailuresAndIgnoreSuccesses() throws IOException {
        final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class, () -> bulkRequest(
            Arrays.asList(update("doc1"), update("doc2")),
            Arrays.asList("update", "update"), Arrays.asList(HttpStatus.SC_OK, HttpStatus.SC_BAD_REQUEST),
            Arrays.asList(null, OTHER_ERROR)));
        //Only the failed item is reported; the successful one carries no error to report
        assertEquals(Collections.singletonList(OTHER_ERROR), e.getFailedItems());
        //and only its document is named, so that a reattempt can leave the other one out
        assertEquals(Collections.singletonMap(TYPE, Collections.singleton("doc2")), e.getFailedDocumentsByStore());
    }

    @Test
    public void shouldNameTheDocumentsOfTheChunksWhichWereNeverSent() throws IOException {
        //A chunk limit of exactly one item's size splits two items into two chunks. The first chunk fails, so the
        //second is never sent, and its document is neither applied nor failed: a reattempt has to resend it, and
        //the failure has to say so
        final int oneItem;
        try (RestElasticSearchClient measuring = createClient()) {
            oneItem = measuring.new RequestBytes(update("doc1")).getSerializedSize();
        }
        try (RestElasticSearchClient clientUnderTest = createClient(oneItem)) {
            final Response firstChunkResponse = bulkResponseWith(Collections.singletonList("update"),
                Collections.singletonList(HttpStatus.SC_BAD_REQUEST), Collections.singletonList(OTHER_ERROR));
            when(restClientMock.performRequest(any())).thenReturn(firstChunkResponse);

            final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class,
                () -> clientUnderTest.bulkRequest(Arrays.asList(update("doc1"), update("doc2")), null));

            assertEquals(Collections.singletonMap(TYPE, Collections.singleton("doc1")), e.getFailedDocumentsByStore());
            assertEquals(Collections.singletonMap(TYPE, Collections.singleton("doc2")), e.getUnsentDocumentsByStore());
            //and the second chunk was indeed never sent
            verify(restClientMock, times(1)).performRequest(any());
        }
    }

    @Test
    public void shouldNameAnOversizedDocumentAsUnsentWhenAnEarlierChunkFails() throws IOException {
        //An item larger than the chunk limit is never sent, and is reported only once every well sized chunk went
        //through. A failure before that has to name its document as unsent, or a reattempt would take it for applied
        //and the mutation would neither be written nor reported
        final int oneItem;
        try (RestElasticSearchClient measuring = createClient()) {
            oneItem = measuring.new RequestBytes(update("doc1")).getSerializedSize();
        }
        final ElasticSearchMutation oversized = ElasticSearchMutation.createUpdateRequest(INDEX, TYPE, "big",
            ImmutableMap.builder().put("doc", ImmutableMap.of("name", Strings.repeat("x", 2 * oneItem))), null);
        try (RestElasticSearchClient clientUnderTest = createClient(oneItem)) {
            final Response firstChunkResponse = bulkResponseWith(Collections.singletonList("update"),
                Collections.singletonList(HttpStatus.SC_BAD_REQUEST), Collections.singletonList(OTHER_ERROR));
            when(restClientMock.performRequest(any())).thenReturn(firstChunkResponse);

            final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class,
                () -> clientUnderTest.bulkRequest(Arrays.asList(update("doc1"), oversized), null));

            assertEquals(Collections.singletonMap(TYPE, Collections.singleton("doc1")), e.getFailedDocumentsByStore());
            assertEquals(Collections.singletonMap(TYPE, Collections.singleton("big")), e.getUnsentDocumentsByStore());
            verify(restClientMock, times(1)).performRequest(any());
        }
    }

    @Test
    public void shouldTreatADeletionAlongsideAFailedUpdateAsOnlyOneFailure() throws IOException {
        final ElasticSearchBulkFailureException e = assertThrows(ElasticSearchBulkFailureException.class, () -> bulkRequest(
            Arrays.asList(ElasticSearchMutation.createDeleteRequest(INDEX, TYPE, "doc1"), update("doc1")),
            Arrays.asList("delete", "update"), Arrays.asList(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_NOT_FOUND),
            Arrays.asList(WHOLE_DELETION_ERROR, ADDITION_ERROR)));
        //The delete is exempt, the update is not
        assertEquals(Collections.singletonList(ADDITION_ERROR), e.getFailedItems());
    }
}
