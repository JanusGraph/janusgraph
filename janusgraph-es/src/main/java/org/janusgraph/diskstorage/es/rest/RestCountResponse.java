// Copyright 2019 JanusGraph Authors
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
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * The answer to a count, which is a search for no hits that counts them all: their total, whether a shard ran out of
 * time, and the shards which answered.
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class RestCountResponse {

    private long count;

    @JsonProperty("timed_out")
    private boolean timedOut;

    @JsonProperty("_shards")
    private RestShardStatistics shards;

    public long getCount() {
        return count;
    }

    //The total is a number on Elasticsearch 6, and an object which holds it as its value since 7
    @JsonProperty("hits")
    public void setHits(Map<String, Object> hits) {
        final Object total = hits == null ? null : hits.get("total");
        final Object value = total instanceof Map ? ((Map<?, ?>) total).get("value") : total;
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("The answer to a count holds no total of its hits: " + hits);
        }
        count = ((Number) value).longValue();
    }

    /**
     * Whether a shard ran out of the count's time, and answered with what it counted until then.
     */
    public boolean isTimedOut() {
        return timedOut;
    }

    /**
     * The shards the count ran on and which of them answered; null where the response doesn't tell.
     */
    public RestShardStatistics getShards() {
        return shards;
    }

    public void setShards(RestShardStatistics shards) {
        this.shards = shards;
    }
}
