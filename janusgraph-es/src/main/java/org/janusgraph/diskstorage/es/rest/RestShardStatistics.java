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

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The {@code _shards} section of the answer to a search, a page of a scroll or of a point in time, or a count: how
 * many shards the request ran on, how many of them answered, and why others didn't.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RestShardStatistics {

    private int total;

    //Shards which were skipped because they can't match are counted here too
    private int successful;

    private int failed;

    //One for each index, type and reason of failure, as Elasticsearch groups them. OpenSearch counts a shard which
    //wasn't available neither among the successful nor among the failed shards, and lists no failure for it
    private List<Map<String, Object>> failures;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getSuccessful() {
        return successful;
    }

    public void setSuccessful(int successful) {
        this.successful = successful;
    }

    public int getFailed() {
        return failed;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public List<Map<String, Object>> getFailures() {
        return failures == null ? Collections.emptyList() : failures;
    }

    public void setFailures(List<Map<String, Object>> failures) {
        this.failures = failures;
    }

    /**
     * Whether every shard the request ran on answered: none failed, and none was left out.
     */
    public boolean isComplete() {
        return failed == 0 && successful >= total;
    }

    /**
     * The failures listed, each as the index and shard which failed, where the answer names them, and as the type
     * and reason of the failure and of its cause, for example
     * {@code [janusgraph_vertex][0] script_exception: runtime error (caused by illegal_argument_exception: boom)};
     * an empty string where none is listed.
     */
    public String describeFailures() {
        return getFailures().stream().map(RestShardStatistics::describe).collect(Collectors.joining("; "));
    }

    private static String describe(Map<String, Object> failure) {
        final StringBuilder description = new StringBuilder();
        final Object index = failure.get("index");
        if (index != null) {
            description.append('[').append(index).append(']');
        }
        //A page of a scroll doesn't know which shard a lost context belonged to, and reports shard -1 of no index
        final Object shard = failure.get("shard");
        if (shard instanceof Number && ((Number) shard).intValue() >= 0) {
            description.append('[').append(shard).append(']');
        }
        final Object reason = failure.get("reason");
        if (reason != null && description.length() > 0) {
            description.append(' ');
        }
        if (reason instanceof Map) {
            description.append(describeCause((Map<?, ?>) reason));
            final Object cause = ((Map<?, ?>) reason).get("caused_by");
            if (cause instanceof Map) {
                description.append(" (caused by ").append(describeCause((Map<?, ?>) cause)).append(')');
            }
        } else if (reason != null) {
            description.append(reason);
        }
        return description.toString();
    }

    //A shard which wasn't available fails without a reason
    private static String describeCause(Map<?, ?> cause) {
        final Object reason = cause.get("reason");
        return reason == null ? String.valueOf(cause.get("type")) : cause.get("type") + ": " + reason;
    }
}
