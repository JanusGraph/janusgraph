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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

//BackendOperation reattempts a temporary failure by calling mutate again with the same mutation map. Before the
//failure is rethrown, mutate takes out of that map what is known to have applied, so that the reattempt does not
//write it a second time: the values of a LIST cardinality property are appended by the addition script, so a document
//written twice holds them twice
public class ElasticSearchIndexReattemptTest {

    private static final Set<String> NO_STORES = Collections.emptySet();

    private static IndexMutation mutation() {
        return new IndexMutation(key -> null, false, false);
    }

    //store -> document -> mutation, mutable at both levels like the map IndexTransaction hands over
    private static Map<String, Map<String, IndexMutation>> pending(Map<String, Set<String>> documentsByStore) {
        final Map<String, Map<String, IndexMutation>> pending = new HashMap<>();
        documentsByStore.forEach((store, documents) -> {
            final Map<String, IndexMutation> mutations = new HashMap<>();
            documents.forEach(document -> mutations.put(document, mutation()));
            pending.put(store, mutations);
        });
        return pending;
    }

    private static Map<String, Set<String>> documents(Map<String, Map<String, IndexMutation>> pending) {
        final Map<String, Set<String>> documents = new HashMap<>();
        pending.forEach((store, mutations) -> documents.put(store, mutations.keySet()));
        return documents;
    }

    private static ElasticSearchBulkFailureException bulkFailure(Map<String, Set<String>> failedDocumentsByStore) {
        return new ElasticSearchBulkFailureException(ImmutableSet.of(429), Collections.singletonList("throttled"),
            failedDocumentsByStore);
    }

    private static ElasticSearchBulkFailureException bulkFailure(Map<String, Set<String>> failedDocumentsByStore,
                                                                 Map<String, Set<String>> unsentDocumentsByStore) {
        return new ElasticSearchBulkFailureException(ImmutableSet.of(429), Collections.singletonList("throttled"),
            failedDocumentsByStore, unsentDocumentsByStore);
    }

    @Test
    public void shouldKeepTheDocumentsOfTheChunksWhichWereNeverSent() {
        final Map<String, Map<String, IndexMutation>> pending = pending(ImmutableMap.of(
            "vertex", ImmutableSet.of("failed", "applied", "unsent")));

        //The bulk was split into chunks; the first failed after "applied" went through, so the second was never sent
        ElasticSearchIndex.retainUnappliedDocuments(pending, NO_STORES, ImmutableSet.of("vertex"),
            bulkFailure(ImmutableMap.of("vertex", ImmutableSet.of("failed")),
                ImmutableMap.of("vertex", ImmutableSet.of("unsent"))));

        assertEquals(ImmutableMap.of("vertex", ImmutableSet.of("failed", "unsent")), documents(pending));
    }

    @Test
    public void shouldTakeOutTheStoresWhoseBulkRequestReturned() {
        final Map<String, Map<String, IndexMutation>> pending = pending(ImmutableMap.of(
            "applied", ImmutableSet.of("a1", "a2"), "failed", ImmutableSet.of("f1")));

        ElasticSearchIndex.retainUnappliedDocuments(pending, ImmutableSet.of("applied"), ImmutableSet.of("failed"),
            bulkFailure(ImmutableMap.of("failed", ImmutableSet.of("f1"))));

        assertEquals(ImmutableMap.of("failed", ImmutableSet.of("f1")), documents(pending));
    }

    @Test
    public void shouldKeepOnlyTheFailedDocumentsOfTheBulkWhichFailed() {
        final Map<String, Map<String, IndexMutation>> pending = pending(ImmutableMap.of(
            "vertex", ImmutableSet.of("good", "bad"), "edge", ImmutableSet.of("fine")));
        final IndexMutation bad = pending.get("vertex").get("bad");

        ElasticSearchIndex.retainUnappliedDocuments(pending, NO_STORES, ImmutableSet.of("vertex", "edge"),
            bulkFailure(ImmutableMap.of("vertex", ImmutableSet.of("bad"))));

        //The edge store had no failed item, so it is gone altogether rather than left empty
        assertEquals(ImmutableMap.of("vertex", ImmutableSet.of("bad")), documents(pending));
        assertSame(bad, pending.get("vertex").get("bad"));
    }

    @Test
    public void shouldKeepTheWholeBulkAfterAFailureWithoutAResponse() {
        final Map<String, Map<String, IndexMutation>> pending = pending(ImmutableMap.of(
            "applied", ImmutableSet.of("a1"), "vertex", ImmutableSet.of("good", "bad")));

        ElasticSearchIndex.retainUnappliedDocuments(pending, ImmutableSet.of("applied"), ImmutableSet.of("vertex"),
            new ConnectException("connection refused"));

        //Nothing is known about the bulk which was interrupted, so all of it is resent; the earlier bulk did return
        assertEquals(ImmutableMap.of("vertex", ImmutableSet.of("good", "bad")), documents(pending));
    }

    @Test
    public void shouldKeepTheWholeBulkWhenTheFailureNamesADocumentTheMutationDoesNotHold() {
        final Map<String, Map<String, IndexMutation>> pending = pending(ImmutableMap.of(
            "vertex", ImmutableSet.of("good", "bad")));

        ElasticSearchIndex.retainUnappliedDocuments(pending, NO_STORES, ImmutableSet.of("vertex"),
            bulkFailure(ImmutableMap.of("vertex", ImmutableSet.of("unknown"))));

        //Narrowing to a document which is not there would leave the reattempt nothing to resend
        assertEquals(ImmutableMap.of("vertex", ImmutableSet.of("good", "bad")), documents(pending));
    }

    @Test
    public void shouldStillTakeOutTheAppliedStoresWhenADocumentMapMayNotChange() {
        final Map<String, Map<String, IndexMutation>> pending = pending(ImmutableMap.of(
            "applied", ImmutableSet.of("a1"), "vertex", ImmutableSet.of("good", "bad")));
        pending.put("vertex", Collections.unmodifiableMap(pending.get("vertex")));

        ElasticSearchIndex.retainUnappliedDocuments(pending, ImmutableSet.of("applied"), ImmutableSet.of("vertex"),
            bulkFailure(ImmutableMap.of("vertex", ImmutableSet.of("bad"))));

        //Every removal takes out only what applied, so what was removed before the map refused stands: the applied
        //store is gone, and the bulk which could not be narrowed is resent whole
        assertEquals(ImmutableMap.of("vertex", ImmutableSet.of("good", "bad")), documents(pending));
    }

    @Test
    public void shouldLeaveAMapItMayNotChangeAlone() {
        final Map<String, Map<String, IndexMutation>> pending = Collections.unmodifiableMap(pending(ImmutableMap.of(
            "applied", ImmutableSet.of("a1"), "vertex", ImmutableSet.of("good", "bad"))));

        //No exception, and nothing inside the map is narrowed either, although the document maps themselves could
        //be: the caller has the whole mutation resent
        ElasticSearchIndex.retainUnappliedDocuments(pending, ImmutableSet.of("applied"), ImmutableSet.of("vertex"),
            bulkFailure(ImmutableMap.of("vertex", ImmutableSet.of("bad"))));

        assertEquals(ImmutableMap.of("applied", ImmutableSet.of("a1"), "vertex", ImmutableSet.of("good", "bad")),
            documents(pending));
    }
}
