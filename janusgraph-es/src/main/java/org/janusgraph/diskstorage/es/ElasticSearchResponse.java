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

package org.janusgraph.diskstorage.es;

import org.janusgraph.diskstorage.indexing.RawQuery;

import java.util.List;
import java.util.stream.Stream;

public class ElasticSearchResponse {

    private long took;

    private String scrollId;

    private List<RawQuery.Result<String>> results;

    private String pitId;

    private List<Object> lastSort;

    public long getTook() {
        return took;
    }

    public void setTook(long took) {
        this.took = took;
    }

    public Stream<RawQuery.Result<String>> getResults() {
        return results.stream();
    }

    public void setResults(List<RawQuery.Result<String>> results) {
        this.results = results;
    }

    public int numResults() {
        return results.size();
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    /**
     * The id of the point in time a search of one has to use next, which may differ from the one it was given.
     */
    public String getPitId() {
        return pitId;
    }

    public void setPitId(String pitId) {
        this.pitId = pitId;
    }

    /**
     * The sort values of the last hit, after which the next page of a search of a point in time is asked for; null
     * without hits or sort values. A response read from Elasticsearch takes them from its last hit; the setter serves
     * another implementation, and tests.
     */
    public List<Object> getLastSort() {
        return lastSort;
    }

    public void setLastSort(List<Object> lastSort) {
        this.lastSort = lastSort;
    }
}
