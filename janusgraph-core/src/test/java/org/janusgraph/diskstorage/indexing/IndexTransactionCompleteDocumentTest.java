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

package org.janusgraph.diskstorage.indexing;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//The complete indexed content of an existing element travels with its mutation to the provider, which uses it to
//recreate a document it finds missing. Only an update of an existing document takes it: a new document is written
//whole anyway, and a deleted one is not recreated
@ExtendWith(MockitoExtension.class)
public class IndexTransactionCompleteDocumentTest {

    private static final List<IndexEntry> COMPLETE = Arrays.asList(new IndexEntry("name", "whole"),
        new IndexEntry("age", 2));

    @Mock
    private IndexProvider index;

    @Mock
    private KeyInformation.IndexRetriever keyInformation;

    @Mock
    private KeyInformation.StoreRetriever storeRetriever;

    @Mock
    private BaseTransactionConfigurable providerTx;

    @Captor
    private ArgumentCaptor<Map<String, Map<String, IndexMutation>>> mutations;

    private IndexTransaction tx;

    @BeforeEach
    public void openTransaction() throws BackendException {
        final BaseTransactionConfig config = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        when(index.beginTransaction(config)).thenReturn(providerTx);
        lenient().when(keyInformation.get("store")).thenReturn(storeRetriever);
        tx = new IndexTransaction(index, keyInformation, config, Duration.ofSeconds(5));
    }

    @Test
    public void shouldAttachItToTheUpdateOfAnExistingDocument() throws BackendException {
        tx.add("store", "doc", "age", 2, false);
        tx.registerCompleteDocument("store", "doc", COMPLETE);
        assertTrue(tx.hasCompleteDocument("store", "doc"));

        tx.commit();

        verify(index).mutate(mutations.capture(), eq(keyInformation), eq(providerTx));
        final IndexMutation mutation = mutations.getValue().get("store").get("doc");
        assertTrue(mutation.hasCompleteDocument());
        assertEquals(COMPLETE, mutation.getCompleteDocument());
    }

    @Test
    public void shouldTreatAReplacedRelationAsAnUpdateOfAnExistingDocument() throws BackendException {
        //A property change on an edge or a vertex property removes the relation and adds a new one with the same id:
        //the deletion comes from a removed element, the addition from a new one, and the two add up to an update of
        //the existing document, which is what needsCompleteDocument answers
        tx.delete("store", "doc", "age", 1, true);
        assertFalse(tx.needsCompleteDocument("store", "doc"));
        tx.add("store", "doc", "age", 2, true);
        assertTrue(tx.needsCompleteDocument("store", "doc"));

        tx.registerCompleteDocument("store", "doc", COMPLETE);
        assertFalse(tx.needsCompleteDocument("store", "doc"));

        tx.commit();

        verify(index).mutate(mutations.capture(), eq(keyInformation), eq(providerTx));
        assertEquals(COMPLETE, mutations.getValue().get("store").get("doc").getCompleteDocument());
    }

    @Test
    public void mergeShouldKeepTheCompleteDocumentSuppliedLast() {
        //Two mutations of one document can only carry snapshots of the same element; the later one stands, and one
        //without a snapshot leaves the existing one in place
        final IndexMutation first = new IndexMutation(storeRetriever, false, false);
        first.setCompleteDocument(Collections.singletonList(new IndexEntry("age", 1)));
        final IndexMutation second = new IndexMutation(storeRetriever, false, false);
        second.setCompleteDocument(COMPLETE);

        first.merge(second);
        assertEquals(COMPLETE, first.getCompleteDocument());

        first.merge(new IndexMutation(storeRetriever, false, false));
        assertEquals(COMPLETE, first.getCompleteDocument());
    }

    @Test
    public void shouldTakeAnEmptyCompleteDocumentAsSuppliedOnce() throws BackendException {
        //An element left with nothing indexed has an empty complete document. It counts as supplied, so the caller does
        //not read the element again for every further update of the document, which would read the same state
        tx.delete("store", "doc", "age", 1, false);
        tx.registerCompleteDocument("store", "doc", Collections.emptyList());
        assertTrue(tx.hasCompleteDocument("store", "doc"));
        assertFalse(tx.needsCompleteDocument("store", "doc"));

        tx.commit();

        verify(index).mutate(mutations.capture(), eq(keyInformation), eq(providerTx));
        assertEquals(Collections.emptyList(), mutations.getValue().get("store").get("doc").getCompleteDocument());
    }

    @Test
    public void shouldNotAttachItToANewDocument() {
        tx.add("store", "doc", "age", 2, true);
        assertFalse(tx.needsCompleteDocument("store", "doc"));
        tx.registerCompleteDocument("store", "doc", COMPLETE);
        assertFalse(tx.hasCompleteDocument("store", "doc"));
    }

    @Test
    public void shouldNotAttachItToADeletedDocument() {
        tx.delete("store", "doc", "age", 1, true);
        tx.registerCompleteDocument("store", "doc", COMPLETE);
        assertFalse(tx.hasCompleteDocument("store", "doc"));
    }

    @Test
    public void shouldIgnoreADocumentWithoutAMutation() throws BackendException {
        tx.registerCompleteDocument("store", "doc", COMPLETE);
        assertFalse(tx.hasCompleteDocument("store", "doc"));

        tx.commit();

        verify(index, never()).mutate(any(), any(), any());
    }
}
