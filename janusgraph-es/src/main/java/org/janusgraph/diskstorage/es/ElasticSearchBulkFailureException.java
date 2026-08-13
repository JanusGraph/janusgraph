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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

//Thrown when items of an Elasticsearch bulk request failed and could not be retried by the client itself. A bulk
//request reports item level failures inside an otherwise successful HTTP response, so the status of a failed item is
//not available from the enclosing response. Retaining those statuses here allows a caller to tell a transient failure
//- a rejected execution while a write queue is saturated, for instance - apart from a permanent one such as a
//mapping conflict
public class ElasticSearchBulkFailureException extends IOException {

    private static final long serialVersionUID = 5060142174725161541L;

    private final Set<Integer> failedItemStatusCodes;

    public ElasticSearchBulkFailureException(String message, Set<Integer> failedItemStatusCodes) {
        super(message);
        this.failedItemStatusCodes = Collections.unmodifiableSet(new HashSet<>(failedItemStatusCodes));
    }

    //The distinct HTTP status codes Elasticsearch reported for the bulk items which failed
    public Set<Integer> getFailedItemStatusCodes() {
        return failedItemStatusCodes;
    }
}
