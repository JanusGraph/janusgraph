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
import org.janusgraph.diskstorage.es.ElasticSearchResponse;
import org.janusgraph.diskstorage.indexing.RawQuery;

import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestSearchResponse extends ElasticSearchResponse {

    private long took;

    @JsonProperty("hits")
    private RestSearchResults hits;

    @JsonProperty("_scroll_id")
    private String scrollId;

    @Override
    public long getTook() {
        return took;
    }

    @Override
    public void setTook(long took) {
        this.took = took;
    }

    public RestSearchResults getHits() {
        return hits;
    }

    public void setHits(RestSearchResults hits) {
        this.hits = hits;
    }

    public int getNumHits() {
        return hits.getHits().size();
    }

    public Float getMaxScore() {
        return hits.getMaxScore();
    }

    @Override
    public Stream<RawQuery.Result<String>> getResults() {
        return hits.getHits().stream()
            .map(hit -> new RawQuery.Result<>(hit.getId(), hit.getScore() != null ? hit.getScore() : 0f));
    }

    @Override
    public int numResults() {
        return hits.getHits().size();
    }

    @Override
    public String getScrollId() {
        return scrollId;
    }

    @Override
    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }
}
