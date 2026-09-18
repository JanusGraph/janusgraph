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

import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

//A bulk response reports item level failures inside an otherwise successful HTTP response. Every 404 used to be treated
//as a success regardless of the mutation which produced it. That is right for a mutation which only removes content,
//because an absent document already satisfies it, and both a whole document deletion and a script which deletes fields
//qualify. It is wrong for a mutation which adds content: there the 404 is a document_missing_exception and the write did
//not happen. A property change on a SINGLE cardinality key produces a field deletion and an addition against the same
//document, and mutate() withholds the upsert from the addition, so the addition is the item worth reporting.
@ExtendWith(MockitoExtension.class)
public class RestClientBulkItemStatusTest {

    private static final String INDEX = "some_index";
    private static final String TYPE = "some_type";

    @Mock
    private RestClient restClientMock;

    @Mock
    private StatusLine statusLine;

    private RestElasticSearchClient createClient() throws IOException {
        when(restClientMock.performRequest(any())).thenThrow(new IOException());
        final RestElasticSearchClient clientUnderTest = new RestElasticSearchClient(restClientMock, 0, false,
            0, Collections.emptySet(), 0, 0, 100_000_000);
        Mockito.reset(restClientMock);
        return clientUnderTest;
    }

    //Builds a bulk response in which each submitted item reports the given status, and a non-null error when the
    //status is a failure
    private Response bulkResponseWith(List<String> operations, List<Integer> statuses) throws IOException {
        final RestBulkResponse bulkResponse = new RestBulkResponse();
        final List<Map<String, RestBulkResponse.RestBulkItemResponse>> items = new ArrayList<>();
        for (int i = 0; i < operations.size(); i++) {
            final RestBulkResponse.RestBulkItemResponse item = new RestBulkResponse.RestBulkItemResponse();
            final int status = statuses.get(i);
            item.setStatus(status);
            if (status >= 300) {
                item.setError(status == 404 ? "document_missing_exception" : "an_error");
            }
            items.add(Collections.singletonMap(operations.get(i), item));
        }
        bulkResponse.setItems(items);

        final HttpEntity entity = Mockito.mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(new ObjectMapper().writeValueAsBytes(bulkResponse)));
        when(statusLine.getStatusCode()).thenReturn(200);
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
        try (RestElasticSearchClient clientUnderTest = createClient()) {
            //Built before the stubbing below, because it stubs mocks of its own
            final Response bulkResponse = bulkResponseWith(operations, statuses);
            when(restClientMock.performRequest(any())).thenReturn(bulkResponse);
            clientUnderTest.bulkRequest(mutations, null);
        }
    }

    @Test
    public void shouldTreatTheDeletionOfAnAbsentDocumentAsSuccess() throws IOException {
        //Nothing is thrown: the index is already in the state the deletion asked for
        bulkRequest(Collections.singletonList(ElasticSearchMutation.createDeleteRequest(INDEX, TYPE, "doc1")),
            Collections.singletonList("delete"), Collections.singletonList(404));
    }

    @Test
    public void shouldTreatTheFieldDeletionOfAnAbsentDocumentAsSuccess() throws IOException {
        //Nothing is thrown: a document with no fields left to delete is the state the mutation asked for. Elasticsearch
        //reports this as an update of a missing document, the same way it reports an addition which was lost
        bulkRequest(Collections.singletonList(fieldDeletion("doc1")),
            Collections.singletonList("update"), Collections.singletonList(404));
    }

    @Test
    public void shouldReportAnUpdateOfAMissingDocument() throws IOException {
        final IOException e = assertThrows(IOException.class,
            () -> bulkRequest(Collections.singletonList(update("doc1")),
                Collections.singletonList("update"), Collections.singletonList(404)));
        assertTrue(e.getMessage().contains("document_missing_exception"), e.getMessage());
    }

    @Test
    public void shouldReportAnIndexRequestWhichReturnedNotFound() throws IOException {
        //An index request creates the document, so a 404 is not something it can ask for
        assertThrows(IOException.class, () -> bulkRequest(
            Collections.singletonList(ElasticSearchMutation.createIndexRequest(INDEX, TYPE, "doc1",
                ImmutableMap.of("name", "value"))),
            Collections.singletonList("index"), Collections.singletonList(404)));
    }

    @Test
    public void shouldReportOnlyTheAdditionWhenAFieldDeletionAccompaniesIt() throws IOException {
        //This is the shape mutate() produces for a value change on a SINGLE cardinality key: the field deletion script
        //and the addition script against the same absent document. mutate() leaves the addition without an upsert once
        //the mutation has deletions, so the addition is the half which lost a write and the only half worth reporting
        final IOException e = assertThrows(IOException.class, () -> bulkRequest(
            Arrays.asList(fieldDeletion("doc1"), update("doc1")),
            Arrays.asList("update", "update"), Arrays.asList(404, 404)));
        assertEquals(1, e.getMessage().split("document_missing_exception", -1).length - 1, e.getMessage());
    }

    @Test
    public void shouldStillReportOtherFailuresAndIgnoreSuccesses() throws IOException {
        final IOException e = assertThrows(IOException.class, () -> bulkRequest(
            Arrays.asList(update("doc1"), update("doc2")),
            Arrays.asList("update", "update"), Arrays.asList(200, 400)));
        assertTrue(e.getMessage().contains("an_error"), e.getMessage());
        //Only the failed item is reported
        assertEquals(1, e.getMessage().split("an_error", -1).length - 1, e.getMessage());
    }

    @Test
    public void shouldTreatADeletionAlongsideAFailedUpdateAsOnlyOneFailure() throws IOException {
        final IOException e = assertThrows(IOException.class, () -> bulkRequest(
            Arrays.asList(ElasticSearchMutation.createDeleteRequest(INDEX, TYPE, "doc1"), update("doc1")),
            Arrays.asList("delete", "update"), Arrays.asList(404, 404)));
        //The delete is exempt, the update is not
        assertEquals(1, e.getMessage().split("document_missing_exception", -1).length - 1, e.getMessage());
    }
}
