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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

//Thrown when items of an Elasticsearch bulk request failed and could not be retried by the client itself. A bulk
//request reports item level failures inside an otherwise successful HTTP response, so the status of a failed item is
//not available from the enclosing response. Retaining those statuses here allows a caller to tell a transient failure
//- a rejected execution while a write queue is saturated, for instance - apart from a permanent one such as a
//mapping conflict
public class ElasticSearchBulkFailureException extends IOException {

    private static final long serialVersionUID = 5060142174725161541L;

    //A batch can hold as many items as the chunk limit allows, so rendering all of them into the message would
    //produce a string large enough to matter once a caller logs it, and this failure is now reattempted
    private static final int MAX_ITEMS_IN_MESSAGE = 5;

    private final Set<Integer> failedItemStatusCodes;

    private final List<Object> failedItems;

    //The ids of the documents whose items failed, by store, and of the documents in the chunks of the request which
    //were never sent because of that failure, so that a reattempt can leave out the documents which applied and
    //nothing else. Both empty when the failure did not come with that information
    private final Map<String, Set<String>> failedDocumentsByStore;
    private final Map<String, Set<String>> unsentDocumentsByStore;

    public ElasticSearchBulkFailureException(Set<Integer> failedItemStatusCodes, List<Object> failedItems) {
        this(failedItemStatusCodes, failedItems, Collections.emptyMap(), Collections.emptyMap());
    }

    public ElasticSearchBulkFailureException(Set<Integer> failedItemStatusCodes, List<Object> failedItems,
                                             Map<String, Set<String>> failedDocumentsByStore) {
        this(failedItemStatusCodes, failedItems, failedDocumentsByStore, Collections.emptyMap());
    }

    public ElasticSearchBulkFailureException(Set<Integer> failedItemStatusCodes, List<Object> failedItems,
                                             Map<String, Set<String>> failedDocumentsByStore,
                                             Map<String, Set<String>> unsentDocumentsByStore) {
        super(describe(
            Objects.requireNonNull(failedItemStatusCodes, "The statuses of the failed bulk items are required"),
            Objects.requireNonNull(failedItems, "The failed bulk items are required")));
        this.failedItemStatusCodes = Collections.unmodifiableSet(new HashSet<>(failedItemStatusCodes));
        this.failedItems = Collections.unmodifiableList(new ArrayList<>(failedItems));
        this.failedDocumentsByStore = copyOf(failedDocumentsByStore, "failed");
        this.unsentDocumentsByStore = copyOf(unsentDocumentsByStore, "unsent");
    }

    private static Map<String, Set<String>> copyOf(Map<String, Set<String>> documentsByStore, String what) {
        final Map<String, Set<String>> documents = new HashMap<>();
        Objects.requireNonNull(documentsByStore, () -> "The " + what + " documents are required").forEach((store, ids) -> {
            Objects.requireNonNull(store, () -> "A store of the " + what + " documents is required");
            Objects.requireNonNull(ids, () -> "The " + what + " documents of store " + store + " are required");
            ids.forEach(id -> Objects.requireNonNull(id,
                () -> "A document id of the " + what + " documents of store " + store + " is required"));
            documents.put(store, Collections.unmodifiableSet(new HashSet<>(ids)));
        });
        return Collections.unmodifiableMap(documents);
    }

    private static String describe(Set<Integer> failedItemStatusCodes, List<Object> failedItems) {
        final StringBuilder message = new StringBuilder("Failure(s) in Elasticsearch bulk request: ")
            .append(failedItems.size()).append(" item(s) failed with statuses ").append(failedItemStatusCodes);
        if (!failedItems.isEmpty()) {
            message.append(", starting with ")
                .append(failedItems.subList(0, Math.min(MAX_ITEMS_IN_MESSAGE, failedItems.size())));
            if (failedItems.size() > MAX_ITEMS_IN_MESSAGE) {
                message.append(" and ").append(failedItems.size() - MAX_ITEMS_IN_MESSAGE).append(" more");
            }
        }
        return message.toString();
    }

    //The distinct HTTP status codes Elasticsearch reported for the bulk items which failed
    public Set<Integer> getFailedItemStatusCodes() {
        return failedItemStatusCodes;
    }

    //Every item Elasticsearch reported as failed, whether or not the message rendered it
    public List<Object> getFailedItems() {
        return failedItems;
    }

    //The ids of the documents whose items failed, by store. Empty when the failure carries no such information
    public Map<String, Set<String>> getFailedDocumentsByStore() {
        return failedDocumentsByStore;
    }

    //The ids of the documents in the chunks of the request which were never sent because of this failure, by store.
    //Not applied, so a reattempt has to resend them. Empty when the failure carries no such information
    public Map<String, Set<String>> getUnsentDocumentsByStore() {
        return unsentDocumentsByStore;
    }
}
