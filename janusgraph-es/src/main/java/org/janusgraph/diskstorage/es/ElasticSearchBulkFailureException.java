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
import java.util.HashSet;
import java.util.List;
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

    public ElasticSearchBulkFailureException(Set<Integer> failedItemStatusCodes, List<Object> failedItems) {
        super(describe(
            Objects.requireNonNull(failedItemStatusCodes, "The statuses of the failed bulk items are required"),
            Objects.requireNonNull(failedItems, "The failed bulk items are required")));
        this.failedItemStatusCodes = Collections.unmodifiableSet(new HashSet<>(failedItemStatusCodes));
        this.failedItems = Collections.unmodifiableList(new ArrayList<>(failedItems));
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
}
