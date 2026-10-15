// Copyright 2017 JanusGraph Authors
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

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestAggResponse {
    private RestAggResult aggregations;

    @JsonProperty("timed_out")
    private boolean timedOut;

    @JsonProperty("_shards")
    private RestShardStatistics shards;

    public RestAggResult getAggregations() {
        return aggregations;
    }

    public void setAggregations(RestAggResult aggregations) {
        this.aggregations = aggregations;
    }

    /**
     * Whether a shard ran out of the aggregation's time, and answered with what it aggregated until then.
     */
    public boolean isTimedOut() {
        return timedOut;
    }

    /**
     * The shards the aggregation ran on and which of them answered; null where the response doesn't tell.
     */
    public RestShardStatistics getShards() {
        return shards;
    }

    public void setShards(RestShardStatistics shards) {
        this.shards = shards;
    }
}
